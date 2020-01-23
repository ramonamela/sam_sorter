from uuid import uuid4
import os, errno
import shutil
import os
from math import ceil
from multiprocessing import Pool
import pandas as pd
import csv
from math import ceil
import sys


def count_file_lines(input_file):
    count = 0
    with open(input_file, 'rb') as file:
        buffer = file.read(8192 * 1024)
        while buffer:
            count += buffer.count(b'\n')
            buffer = file.read(8192 * 1024)
    return count


class SamMerger():
    def __init__(self, amount_of_workers=None):
        if amount_of_workers is None:
            amount_of_workers = os.cpu_count()
        self.pool = Pool(processes=amount_of_workers)


    @staticmethod
    def get_column_order(sort_criteria):
        if sort_criteria == "queryname":
            return [0,1], [True, True]
        elif sort_criteria == "coordinate":
            return [2,3], [True, True]
            #return [2, 3, 0], [True, True, True]
        raise("Unknown sort criteria", sort_criteria)


    def split_input(self, input_file, working_dir="/tmp", amount_of_chunks=None, amount_of_workers=None,
                    sort_criteria="queryname"):
        total_amount_lines = count_file_lines(input_file)
        if amount_of_chunks is None:
            ## Compute optimal amount of chunks
            input_file_size = os.stat(input_file).st_size
            total, used, free = shutil.disk_usage(working_dir)
            amount_of_chunks = 1

        # Include header
        total_amount_lines -= 1
        lines_per_file = ceil(float(total_amount_lines) / float(amount_of_chunks))
        files_to_merge = []

        if amount_of_workers is None:
            amount_of_workers = os.cpu_count()

        joined_column = 11

        header = []

        with open(input_file, 'rb') as input_file:
            # We store the commented lines
            previous_position = input_file.tell()
            line = input_file.readline()
            while line.startswith(b"@"):
                header.append(line)
                previous_position = input_file.tell()
                line = input_file.readline()
            input_file.seek(previous_position)

            chunk_counter = 0
            base_output_file = working_dir + "/" + "chunk_counter_"
            new_chunk_file = base_output_file + str(chunk_counter) + ".sam"
            output_file = open(new_chunk_file, 'bw')
            for i, line in enumerate(input_file):
                splitted_line = line.split(b"\t")
                new_splitted_line = splitted_line[:joined_column] + [b"#".join(splitted_line[joined_column:])]
                new_line = b"\t".join(new_splitted_line)
                output_file.write(new_line)
                if (i % lines_per_file) == (lines_per_file - 1):
                    output_file.close()
                    # Store it in this manner so we can wait dinamically afterwards
                    files_to_merge.append((self.pool.apply_async(sort_sam_file, (new_chunk_file,),
                                                            {"sort_criteria": sort_criteria}), new_chunk_file, 0))
                    chunk_counter += 1
                    new_chunk_file = base_output_file + str(chunk_counter) + ".sam"
                    output_file = open(new_chunk_file, 'bw')
            output_file.close()
            files_to_merge.append((self.pool.apply_async(sort_sam_file, (new_chunk_file,), {"sort_criteria":sort_criteria}), new_chunk_file, 0))
            output_file.close()

        return header, files_to_merge


    def merge_chunk_lists(self, left_list, right_list, sort_criteria="queryname"):

        assert (len(left_list) > 0 and len(right_list) > 0)
        result_file_list = []
        # result_file_candidate_list = []

        left_list[0][0].wait()
        right_list[0][0].wait()

        left_item = left_list[0]
        right_item = right_list[0]
        left_list.remove(left_item)
        right_list.remove(right_item)

        candidate_path = right_item[1]
        #first_lines = SamMerger.merge_two_chunks(left_item[1], right_item[1], sort_criteria=sort_criteria)
        first_lines = self.pool.apply_async(SamMerger.merge_two_chunks, (left_item[1], right_item[1]), {"sort_criteria":sort_criteria})
        result_tuple = (first_lines, left_item[1], 0)
        candidate_tuple = (first_lines, candidate_path, 1)
        result_file_list.append(result_tuple)

        while len(left_list) > 0 and len(right_list) > 0:
            first_left_row = left_list[0][0].get()[left_list[0][2]]
            first_right_row = right_list[0][0].get()[right_list[0][2]]

            if compare_rows(first_left_row, first_right_row, sort_criteria=sort_criteria) > -1:
                result_chunk_dest = left_list[0][1]
                left_list.remove(left_list[0])
            else:
                result_chunk_dest = right_list[0][1]
                right_list.remove(right_list[0])

            candidate_tuple[0].wait()
            #first_lines = SamMerger.merge_two_chunks(result_chunk_dest, candidate_path, sort_criteria=sort_criteria)
            first_lines = self.pool.apply_async(SamMerger.merge_two_chunks, (result_chunk_dest, candidate_path),
                                                {"sort_criteria": sort_criteria})
            result_tuple = (first_lines, result_chunk_dest, 0)
            candidate_tuple = (first_lines, candidate_path, 1)
            result_file_list.append(result_tuple)

        while len(left_list) > 0:
            left_list[0][0].wait()
            result_chunk_dest = left_list[0][1]
            left_list.remove(left_list[0])
            candidate_tuple[0].wait()
            first_lines = self.pool.apply_async(SamMerger.merge_two_chunks, (result_chunk_dest, candidate_path),
                                                {"sort_criteria": sort_criteria})
            result_tuple = (first_lines, result_chunk_dest, 0)
            candidate_tuple = (first_lines, candidate_path, 1)
            result_file_list.append(result_tuple)

        while len(right_list) > 0:
            try:
                right_list[0][0].wait()
            except Exception:
                print("Entering into except")

            result_chunk_dest = right_list[0][1]
            right_list.remove(right_list[0])
            candidate_tuple[0].wait()
            first_lines = self.pool.apply_async(SamMerger.merge_two_chunks, (result_chunk_dest, candidate_path),
                                                {"sort_criteria": sort_criteria})
            result_tuple = (first_lines, result_chunk_dest, 0)
            candidate_tuple = (first_lines, candidate_path, 1)
            result_file_list.append(result_tuple)

        result_file_list.append(candidate_tuple)
        return result_file_list


    def recursive_merge(self, chunk_list, sort_criteria="queryname"):
        if len(chunk_list) == 1:
            return chunk_list
        #"""
        chunks_to_merge = list(map(lambda x: [x], chunk_list))

        while len(chunks_to_merge) > 1:
            chunks_to_merge.append(self.merge_chunk_lists(chunks_to_merge[0], chunks_to_merge[1], sort_criteria=sort_criteria))
            chunks_to_merge.pop(0)
            chunks_to_merge.pop(0)

        return chunks_to_merge[0]

        """
        left_list = chunk_list[int(len(chunk_list) / 2):]
        right_list = chunk_list[:int(len(chunk_list) / 2)]
        sorted_left_list = self.recursive_merge(left_list, sort_criteria=sort_criteria)
        sorted_right_list = self.recursive_merge(right_list, sort_criteria=sort_criteria)
        return self.merge_chunk_lists(sorted_left_list, sorted_right_list, sort_criteria=sort_criteria)
        """


    ## The upper hald always in the first chunk!
    @staticmethod
    def merge_two_chunks(first_chunk, second_chunk, sort_criteria="queryname"):
        first_dataframe = pd.read_csv(first_chunk, sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
        second_dataframe = pd.read_csv(second_chunk, sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
        column_names = list(map(lambda x: str(x), list(first_dataframe.columns)))
        first_dataframe.columns = column_names
        second_dataframe.columns = column_names
        columns_order, column_ascending = SamMerger.get_column_order(sort_criteria)
        ordered_column_names = compute_ordered_column_names(columns_order, column_names)

        result_chunk_size = max(first_dataframe.shape[0], second_dataframe.shape[0])

        #print(first_dataframe)
        #print(second_dataframe)
        ordered_dataframe = first_dataframe.merge(second_dataframe, how='outer', left_on=ordered_column_names, right_on=ordered_column_names, sort=True)
        #ordered_dataframe.sort_values(by=ordered_column_names[:len(columns_order)], ascending=column_ascending, inplace=True)

        result_chunk = ordered_dataframe.iloc[:result_chunk_size]

        result_chunk.to_csv(first_chunk, sep="\t", header=False, index=False, quoting=csv.QUOTE_NONE)

        candidate_dataframe = ordered_dataframe.iloc[result_chunk_size:]
        candidate_dataframe.to_csv(second_chunk, sep="\t", header=False, index=False, quoting=csv.QUOTE_NONE)
        return "\t".join(list(map(str, list(ordered_dataframe.iloc[0,:])))), "\t".join(list(map(str, list(candidate_dataframe.iloc[0,:]))))


def sort_sam_file(filename, sort_criteria="queryname"):
    df = pd.read_csv(filename, sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
    column_indexes, column_ascending = SamMerger.get_column_order(sort_criteria)
    columns_considered = list(map(lambda i: list(df.columns)[i], column_indexes))
    df = df.sort_values(by=columns_considered, ascending=column_ascending)
    df.to_csv(filename, sep="\t", header=False, index=False, quoting=csv.QUOTE_NONE)
    return ("\t".join(list(map(str, list(df.loc[0,:])))),)


def compare_rows(first, second, sort_criteria="queryname"):
    ##  1 <- first <  second
    ##  0 <- first == second
    ## -1 <- first >  second
    column_order, column_ascending = SamMerger.get_column_order(sort_criteria)
    first_list = first.split("\t")
    second_list = second.split("\t")
    for col in column_order:
        if first_list[col] < second_list[col]:
            return 1
        elif first_list[col] > second_list[col]:
            return -1
    return 0


def compute_ordered_column_names(columns_order, column_names):
    ordered_columns = []
    for elem in columns_order:
        ordered_columns.append(column_names[elem])
    for elem in column_names:
        if not elem in ordered_columns:
            ordered_columns.append(elem)
    return ordered_columns


def main(input_file, output_file, working_dir, sort_criteria="queryname"):
    new_folder = str(uuid4())
    working_folder = working_dir + "/" + new_folder

    try:
        os.makedirs(working_folder)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    current_merger = SamMerger()

    header, files_to_merge = current_merger.split_input(input_file, working_dir=working_folder, sort_criteria=sort_criteria)

    result_list = current_merger.recursive_merge(files_to_merge, sort_criteria=sort_criteria)

    with open(output_file, 'w') as out_stream:
        for elem in result_list:
            try:
                elem[0].wait()
            except:
                pass
            df = pd.read_csv(elem[1], sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
            for line in df.values:
                out_stream.write('\t'.join(list(map(str, line[:-1])) + line[-1].split("#")) + "\n")


if __name__ == "__main__":
    if len(sys.argv) == 5:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        sort_criteria = sys.argv[3]
        tmp_folder = sys.argv[4]
    else:
        input_file = "/home/ramela/Downloads/mergeSortSam/MergeSortSam/data/test_small.sam"
        output_file = "/home/ramela/Downloads/mergeSortSam/MergeSortSam/result/ramon_sorted_small_qn.sam"
        sort_criteria = "queryname"
        tmp_folder = "/home/ramela/tmp/sam_sorter/"
        #output_file = "/home/ramela/Downloads/mergeSortSam/MergeSortSam/result/ramon_sorted_small_co.sam"
        #sort_criteria = "coordinate"

        #input_file = "/home/ramela/Downloads/mergeSortSam/MergeSortSam/data/test.sam"
        #output_file = "/home/ramela/Downloads/mergeSortSam/MergeSortSam/result/ramon_sorted_qn.sam"
        #sort_criteria = "queryname"
        #output_file = "/home/ramela/Downloads/mergeSortSam/MergeSortSam/result/ramon_sorted_co.sam"
        #sort_criteria = "coordinate"
    import time
    times = []
    for i in range(1):
        start_time = time.time()
        main(input_file, output_file, tmp_folder, sort_criteria=sort_criteria)
        times.append(time.time() - start_time)
    print("Mean time:", sum(times)/len(times))