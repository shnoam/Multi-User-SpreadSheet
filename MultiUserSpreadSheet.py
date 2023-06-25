import sys
from copy import copy
from threading import Lock, Semaphore
from tkinter import *
import random
from concurrent.futures import ThreadPoolExecutor


class SharableSpreadSheet:
    data = None

    def __init__(self, nRows, nCols):

        self.data = [[""] * nCols for row in range(nRows)]

        self.safe_to_read = Lock()
        self.cross = Lock()
        self.rowSem = [Semaphore() for row in range(nRows)]
        self.colSem = [Semaphore() for col in range(nCols)]
        self.write = Semaphore()


        self.num_of_readers = 0
        self.rows_counter = [0 for row in range(nRows)]
        self.col_counter = [0 for col in range(nCols)]

    def get_cell(self, row, cols):

        if row < 0 or row >= len(self.data) or cols < 0 or cols >= len(self.data[0]):
            return None

        self.safe_to_read.acquire()
        self.num_of_readers += 1
        if self.num_of_readers == 1:  # the first reader blocks the others from writing
            self.write.acquire()
        self.safe_to_read.release()  # allows others to read

        "checks if specific cell available for reading"
        self.cross.acquire()
        self.col_counter[cols] += 1
        if self.col_counter == 1:
            self.colSem[cols].acquire()
        self.rows_counter[row] += 1
        if self.rows_counter == 1:
            self.rowSem[row].acquire()
        self.cross.release()

        cell = copy(self.data[row][cols])

        self.cross.acquire()
        self.col_counter[cols] -= 1
        if self.col_counter == 0:
            self.colSem[cols].release()
        self.rows_counter[row] -= 1
        if self.rows_counter == 0:
            self.rowSem[row].release()
        self.cross.release()

        self.safe_to_read.acquire()
        self.num_of_readers -= 1

        if self.num_of_readers == 0:  # the last reader allows the others to write
            self.write.release()
        self.safe_to_read.release()  # allows others to read

        return cell

    def set_cell(self, row, col, new_str):
        """
        set the string at [row,col]

        """

        if row < 0 or row >= len(self.data) or col < 0 or col >= len(self.data[0]):
            return False

        "allows readers to read ,so block writing "
        self.safe_to_read.acquire()
        self.num_of_readers += 1
        if self.num_of_readers == 1:  # the first reader blocks the others from writing
            self.write.acquire()
        self.safe_to_read.release()  # allows others to read

        if self.colSem[col].acquire() and self.rowSem[row].acquire():  # true : when its possible to block row , col. if not , waiting here
            self.data[row][col] = new_str

            self.colSem[col].release()
            self.rowSem[row].release()

        self.safe_to_read.acquire()
        self.num_of_readers -= 1
        if self.num_of_readers == 0:  # the last reader allows the others to write
            self.write.release()
        self.safe_to_read.release()  # allows others to read

        return True

    def search_string(self, str_to_search):
        """
        returns the first cell that contains the string [row,col]
        # return [-1,-1] if don't exists
        """

        for row in range(len(self.data)):

            col = self.search_in_row(row, str_to_search)

            if col != -1:
                return [row, col]

        return [-1, -1]  # if not exists

    def exchange_rows(self, row1, row2):
        # exchange the content of row1 and row2
        if row1 < 0 or row2 < 0 or row1 >= len(self.data) or row2 >= len(self.data):
            return False

        self.write.acquire()  #wants to write

        if self.rowSem[row1].acquire() and self.rowSem[row2].acquire():  # if both of them not locked
            temp = self.data[row2]
            self.data[row2] = self.data[row1]
            self.data[row1] = temp

            # exchange is done , can release both Semaphores
            self.rowSem[row1].release()
            self.rowSem[row2].release()
        self.write.release()  # writing is done , release it

        return True

    def exchange_cols(self, col1, col2):
        # exchange the content of col1 and col2
        if col1 < 0 or col2 < 0 or col2 >= len(self.data[0]) or col1 >= len(self.data[0]):
            return False

        self.write.acquire()

        if self.colSem[col1].acquire() and self.colSem[col2].acquire():  # if both of them not blocked
            # needs to exchange each cell separately
            for row in range(len(self.data)):
                temp = self.data[row][col1]
                self.data[row][col1] = self.data[row][col2]
                self.data[row][col2] = temp

                # exchange is done , can release both Semaphores
                self.colSem[col1].release()
                self.colSem[col2].release()
        self.write.release()  # writing is done , release it

        return True

    def search_in_row(self, row_num, str_to_search):

        if row_num < 0 or row_num >= len(self.data):
            return False
        col_index = -1  # initialize in case of not found

        self.safe_to_read.acquire()
        self.num_of_readers += 1
        if self.num_of_readers == 1:  # the first reader blocks the others from writing
            self.write.acquire()
        self.safe_to_read.release()  # allows others to read

        self.cross.acquire()
        self.rows_counter[row_num] += 1
        if self.rows_counter[row_num] == 1:
            self.rowSem[row_num].acquire()
        self.cross.release()

        for col in range(len(self.data[0])):
            if self.data[row_num][col] == str_to_search:
                col_index = col
                break

        self.cross.acquire()
        self.rows_counter[row_num] -= 1
        if self.rows_counter[row_num] == 0:
            self.rowSem[row_num].release()
        self.cross.release()

        self.safe_to_read.acquire()
        self.num_of_readers -= 1
        if self.num_of_readers == 0:  # the last reader allows the others to write
            self.write.release()
        self.safe_to_read.release()

        return col_index

    def search_in_col(self, col_num, str_to_search):
        """
         perform search in specific col, return row number if exists.
         return -1 otherwise

        """
        if col_num < 0 or col_num >= len(self.data[0]):
            return False

        self.safe_to_read.acquire()
        self.num_of_readers += 1
        if self.num_of_readers == 1:  # the first reader blocks the others from writing
            self.write.acquire()
        self.safe_to_read.release()  # allows others to read


        self.cross.acquire()
        self.col_counter[col_num] += 1
        if self.col_counter[col_num] == 1:  # the last reader allows the others to write
            self.colSem[col_num].acquire()
        self.cross.release()  # allows others to read

        row_index = -1  # initialize in case of not found
        for row in range(len(self.data)):
            if self.data[row][col_num] == str_to_search:
                row_index = row
                break

        self.cross.acquire()
        self.col_counter[col_num] -= 1
        if self.col_counter[col_num] == 0:  # the last reader allows the others to write
            self.colSem[col_num].release()
        self.cross.release()  # allows others to read

        self.safe_to_read.acquire()
        self.num_of_readers -= 1
        if self.num_of_readers == 0:  # the last reader allows the others to write
            self.write.release()
        self.safe_to_read.release()

        return row_index

    def search_in_range(self, col1, col2, row1, row2, str_to_search):
        """
        perform search within specific range: [row1:row2,col1:col2]
        includes col1,col2,row1,row2
        return the first cell that contains the string [row,col]
        return [-1,-1] if don't exists

        """
        for row in range(row1, row2 + 1):  # checks each row
            at_col = self.search_in_row(row, str_to_search)
            if col1 <= at_col <= col2:  # checks if the col_index
                return [row, at_col]

        return [-1, -1]

    def add_row(self, row1):
        """
         add a row after row1

        :return: True if insertion went well
        """

        if row1 < 0 or row1 >= len(self.data):
            return False
        self.write.acquire()
        row = ["" for cell in range(len(self.data[0]))]
        self.data.insert(row1 + 1, row)
        self.rowSem.insert(row1 + 1, Semaphore())
        self.rows_counter.insert(row1 + 1, 0)

        self.write.release()

        return True

    def add_col(self, col1):
        """
        add a col after col1

        """

        if col1 < 0 or col1 >= len(self.data[0]):
            return False
        self.write.acquire()
        for cell in range(len(self.data)):
            self.data[cell].insert(col1 + 1, "")
        self.colSem.insert(col1 + 1, Semaphore())
        self.col_counter.insert(col1 + 1, 0)

        self.write.release()

        return True

    def save(self, f_name):
        # save the spreadsheet to a file fileName as following:
        # nRows,nCols
        # row,col, string
        # row,col, string
        # row,col, string
        # For example 50X50 spread sheet size with only 3 cells with strings:
        # 50,50
        # 3,4,"Hi"
        # 5,10,"OOO"
        # 13,2,"EE"
        # you can decide the saved file extension.
        self.write.acquire()
        saved = open(f"{f_name}.txt", "w")
        saved.write(f"{len(self.data)},{len(self.data[0])}\n")
        for row in range(len(self.data)):
            for col in range(len(self.data[0])):
                if self.data[row][col] != "":
                    saved.write(f'{row},{col},"{self.data[row][col]}\n"')
        saved.close()
        self.write.release()

        return True

    def load(self, f_name):
        """
        load the spreadsheet from fileName
        replace the data and size of the current spreadsheet with the loaded data
        """
        import os
        if not os.path.exists(f_name):
            return False
        self.write.acquire()  # acquire for writing operation (all file)
        load_file = open(f_name, "r")
        file_lines = load_file.readlines()
        line_data = file_lines[0].strip().split(",")
        number_rows, number_cols = int(line_data[0]), int(line_data[1])
        self.data = [[""] * number_cols for row in range(number_rows)]
        for i in range(1, len(file_lines)):
            line_data = file_lines[i].strip().split(",")
            row, col = int(line_data[0]), int(line_data[1])
            value = line_data[2].strip('"')
            self.data[row][col] = value
        load_file.close()

        self.write.release()
        return True

    def show(self):
        """
         show the spreadsheet using tkinker.
         tkinker is the default python GUI library.
         as part of the HW you should learn how to use it.
         there are links and simple example in the last practical lesson on model

        """

        root = Tk()

        frame = Frame(root)

        top_frame = Frame(frame)
        main_title = Label(top_frame, text="Noam & Ilay SharableSpreadSheet", font=('Book Antiqua', 30))
        main_title.pack(padx=5, pady=5)
        top_frame.pack(padx=5, pady=5)
        table = Frame(frame)
        for row in range(len(self.data)):
            for col in range(len(self.data[0])):
                cell = Entry(table, width=8, fg='green', font=('Arial', 10))  # fonts
                cell.grid(row=row, column=col)
                cell.insert(0, self.data[row][col])  # insert the values
        table.pack(padx=5, pady=5)

        frame.pack(padx=5, pady=5)
        root.mainloop()

def spread_sheet_tester(nUsers, nTasks, spreadsheet):
    """
    when each user is doing only one task
    """
    # test the spreadsheet with random operations and nUsers threads.
    allocate_task = [random.randrange(10) for i in range(nUsers)]

    def task_bank(func_idx):
        if func_idx == 0:  # get_cell

            return spreadsheet.get_cell(2, 2)
        if func_idx == 1:

            return spreadsheet.set_cell(1, 1, "hi")
        if func_idx == 2:

            return spreadsheet.search_string("love you")
        if func_idx == 3:

            return spreadsheet.exchange_rows(2, 3)
        if func_idx == 4:

            return spreadsheet.exchange_cols(0, 4)
        if func_idx == 5:

            return spreadsheet.search_in_row(2, "my")
        if func_idx == 6:

            return spreadsheet.search_in_col(2, "smile")
        if func_idx == 7:

            return spreadsheet.search_in_range(1, 4, 2, 4, "pizza")
        if func_idx == 8:

            return spreadsheet.add_row(0)
        if func_idx == 9:

            return spreadsheet.add_col(3)

    with ThreadPoolExecutor(max_workers=nUsers) as executor:
        results = executor.map(task_bank, allocate_task)


    return spreadsheet


def spread_sheet_tester2(nUsers, nTasks, spreadsheet):
    allocate_task = [random.randrange(10) for i in range(nUsers)]  # every user gets one number -> one function to do

    def task_bank(index):
        tasks_list = [random.randrange(10) for i in range(nTasks)]

        thread_result_list = []

        for task in tasks_list:
            if task == 0:

                thread_result_list.append(spreadsheet.get_cell(2, 2))
            if task == 1:

                thread_result_list.append(spreadsheet.set_cell(9, 9, "thank you"))
            if task == 2:

                thread_result_list.append(spreadsheet.search_string("thank you"))
            if task == 3:

                thread_result_list.append(spreadsheet.exchange_rows(1, 2))
            if task == 4:

                thread_result_list.append(spreadsheet.exchange_cols(2, 3))
            if task == 5:

                thread_result_list.append(spreadsheet.search_in_row(2, "stav"))
            if task == 6:

                thread_result_list.append(spreadsheet.search_in_col(4, "hey"))
            if task == 7:

                thread_result_list.append(spreadsheet.search_in_range(1, 2, 0, 3, "ilay"))
            if task == 8:

                thread_result_list.append(spreadsheet.add_row(1))
            if task == 9:

                thread_result_list.append(spreadsheet.add_col(4))

        return thread_result_list

    with ThreadPoolExecutor(max_workers=nUsers) as executor:
        results = executor.map(task_bank, allocate_task)

    # test the spreadsheet with random operations and nUsers threads.
    return spreadsheet


def external_test(n_rows, n_cols, n_users, n_tasks):
    test_spread_sheet = SharableSpreadSheet(n_rows, n_cols)
    test_spread_sheet = spread_sheet_tester(n_users, n_tasks, test_spread_sheet)
    test_spread_sheet.show()
    test_spread_sheet.save('external_test_saved.dat')


if __name__ == '__main__':
    if len(sys.argv) == 5:
        external_test(n_rows=sys.argv[1], n_cols=sys.argv[2], n_users=sys.argv[3], n_tasks=sys.argv[4])
    else:
        # # Internal test example (you can change it to check yourself)
        # # create, test and save SharableSpreadSheet
        load_ss = SharableSpreadSheet(40, 40)

        load_ss.load('test1.txt')

        load_ss = spread_sheet_tester2(5, 2, load_ss)
        load_ss.show()
