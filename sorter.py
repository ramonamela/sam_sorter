from uuid import uuid4
import os, errno
import shutil
import os
from math import ceil
from multiprocessing import Pool
import pandas as pd
import csv

"""
class SamMerger():
    def __init__(self, input_file, output_file, working_dir, sort_criterion):
"""

def count_file_lines(input_file):
    count = 0
    with open(input_file, 'rb') as file:
        buffer = file.read(8192 * 1024)
        while buffer:
            count += buffer.count(b'\n')
            buffer = file.read(8192 * 1024)
    return count

def get_column_order(sort_criteria):
    if sort_criteria == "queryname":
        return [0,1], [True, True]
    elif sort_criteria == "coordinate":
        return [2,3], [True, True]
        #return [2, 3, 0], [True, True, True]
    raise("Unknown sort criteria", sort_criteria)

def sort_sam_file(filename, sort_criteria="queryname"):
    df = pd.read_csv(filename, sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
    column_indexes, column_ascending = get_column_order(sort_criteria)
    columns_considered = list(map(lambda i: list(df.columns)[i], column_indexes))
    df = df.sort_values(by=columns_considered, ascending=column_ascending)
    df.to_csv(filename, sep="\t", header=False, index=False, quoting=csv.QUOTE_NONE)

def compare_rows(first, second, sort_criteria="queryname"):
    ##  1 <- first <  second
    ##  0 <- first == second
    ## -1 <- first >  second
    column_order, column_ascending = get_column_order(sort_criteria)
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

"""
## The upper hald always in the first chunk!
def merge_two_chunks(first_chunk, second_chunk, sort_criteria="qy"):
    try:
        first_chunk[0][0].wait()
    except Exception:
        pass
    first_dataframe = pd.read_csv(first_chunk[0][1], sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
    result_file_candidate_list.append(first_chunk[0][1])
    left_list.remove(first_chunk[0])
    
    try:
        right_list[0][0].wait()
    except Exception:
        pass
    second_dataframe = pd.read_csv(right_list[0][1], sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
    result_file_candidate_list.append(right_list[0][1])
    right_list.remove(right_list[0])
    column_names = list(map(lambda x: str(x), list(first_dataframe.columns)))
    first_dataframe.columns = column_names
    second_dataframe.columns = column_names
    columns_order, column_ascending = get_column_order(sort_criteria)
    ordered_column_names = compute_ordered_column_names(columns_order, column_names)

    result_chunk_size = max(first_dataframe.shape[0], second_dataframe.shape[0])
"""

def merge_chunk_lists(left_list, right_list, sort_criteria="queryname"):

    assert(len(left_list) > 0 and len(right_list) > 0)
    result_file_list = []
    result_file_candidate_list = []
    try:
        left_list[0][0].wait()
    except Exception:
        pass
    try:
        right_list[0][0].wait()
    except Exception:
        pass
    result_file_candidate_list.append(left_list[0][1])
    result_file_candidate_list.append(right_list[0][1])

    first_dataframe = pd.read_csv(left_list[0][1], sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
    left_list.remove(left_list[0])
    second_dataframe = pd.read_csv(right_list[0][1], sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
    right_list.remove(right_list[0])
    column_names = list(map(lambda x: str(x), list(first_dataframe.columns)))
    first_dataframe.columns = column_names
    second_dataframe.columns = column_names
    columns_order, column_ascending = get_column_order(sort_criteria)
    ordered_column_names = compute_ordered_column_names(columns_order, column_names)

    result_chunk_size = max(first_dataframe.shape[0], second_dataframe.shape[0])

    ordered_dataframe = first_dataframe.merge(second_dataframe, how='outer', left_on=ordered_column_names, right_on=ordered_column_names, sort=True)
    ordered_dataframe.sort_values(by=ordered_column_names[:len(columns_order)], ascending=column_ascending, inplace=True)

    result_chunk = ordered_dataframe.iloc[:result_chunk_size]
    result_chunk_dest = result_file_candidate_list.pop(0)

    result_chunk.to_csv(result_chunk_dest, sep="\t", header=False, index=False, quoting=csv.QUOTE_NONE)

    result_file_list.append(("token", result_chunk_dest))

    first_dataframe = ordered_dataframe.iloc[result_chunk_size:]

    while len(left_list) > 0 and len(right_list) > 0:
        with open(left_list[0][1]) as left_can:
            first_left_row = left_can.readline()
        with open(right_list[0][1]) as right_can:
            first_right_row = right_can.readline()
        if compare_rows(first_left_row, first_right_row, sort_criteria=sort_criteria) > -1:
            second_dataframe = pd.read_csv(left_list[0][1], sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
            result_file_candidate_list.append(left_list[0][1])
            left_list.remove(left_list[0])
            discarded_row = first_right_row
        else:
            second_dataframe = pd.read_csv(right_list[0][1], sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
            result_file_candidate_list.append(right_list[0][1])
            right_list.remove(right_list[0])
            discarded_row = first_left_row

        first_dataframe.columns = column_names
        second_dataframe.columns = column_names

        result_chunk_size = max(first_dataframe.shape[0], second_dataframe.shape[0])
        ordered_dataframe = first_dataframe.merge(second_dataframe, how='outer', left_on=ordered_column_names,
                                                  right_on=ordered_column_names, sort=True)
        ordered_dataframe.sort_values(by=ordered_column_names[:len(columns_order)], ascending=column_ascending, inplace=True)

        result_chunk = ordered_dataframe.iloc[:result_chunk_size]

        assert(compare_rows("\t".join(list(map(str,result_chunk.iloc[-1].tolist()))), discarded_row, sort_criteria=sort_criteria) > -1)

        result_chunk_dest = result_file_candidate_list.pop(0)
        result_chunk.to_csv(result_chunk_dest, sep="\t", header=False, index=False, quoting=csv.QUOTE_NONE)
        result_file_list.append(("token", result_chunk_dest))

        first_dataframe = ordered_dataframe.iloc[result_chunk_size:]

    while len(left_list) > 0:
        second_dataframe = pd.read_csv(left_list[0][1], sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
        result_file_candidate_list.append(left_list[0][1])
        left_list.remove(left_list[0])

        first_dataframe.columns = column_names
        second_dataframe.columns = column_names

        result_chunk_size = max(first_dataframe.shape[0], second_dataframe.shape[0])
        ordered_dataframe = first_dataframe.merge(second_dataframe, how='outer', left_on=ordered_column_names,
                                                  right_on=ordered_column_names, sort=True)
        ordered_dataframe.sort_values(by=ordered_column_names[:len(columns_order)], ascending=column_ascending, inplace=True)

        result_chunk = ordered_dataframe.iloc[:result_chunk_size]
        result_chunk_dest = result_file_candidate_list.pop(0)
        result_chunk.to_csv(result_chunk_dest, sep="\t", header=False, index=False, quoting=csv.QUOTE_NONE)
        result_file_list.append(("token", result_chunk_dest))

        first_dataframe = ordered_dataframe.iloc[result_chunk_size:]

    while len(right_list) > 0:
        second_dataframe = pd.read_csv(right_list[0][1], sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
        result_file_candidate_list.append(right_list[0][1])
        right_list.remove(right_list[0])

        first_dataframe.columns = column_names
        second_dataframe.columns = column_names

        result_chunk_size = max(first_dataframe.shape[0], second_dataframe.shape[0])
        ordered_dataframe = first_dataframe.merge(second_dataframe, how='outer', left_on=ordered_column_names,
                                                  right_on=ordered_column_names, sort=True)
        ordered_dataframe.sort_values(by=ordered_column_names[:len(columns_order)], ascending=column_ascending, inplace=True)

        result_chunk = ordered_dataframe.iloc[:result_chunk_size]
        result_chunk_dest = result_file_candidate_list.pop(0)
        result_chunk.to_csv(result_chunk_dest, sep="\t", header=False, index=False, quoting=csv.QUOTE_NONE)
        result_file_list.append(("token", result_chunk_dest))

        first_dataframe = ordered_dataframe.iloc[result_chunk_size:]

    result_chunk_dest = result_file_candidate_list.pop(0)
    result_file_list.append(("token", result_chunk_dest))
    first_dataframe.to_csv(result_chunk_dest, sep="\t", header=False, index=False, quoting=csv.QUOTE_NONE)

    return result_file_list

def recursive_merge(chunk_list, sort_criteria="queryname"):
    if len(chunk_list) == 1:
        return chunk_list
    left_list = chunk_list[int(len(chunk_list)/2):]
    right_list = chunk_list[:int(len(chunk_list)/2)]
    sorted_left_list = recursive_merge(left_list, sort_criteria=sort_criteria)
    sorted_right_list = recursive_merge(right_list, sort_criteria=sort_criteria)
    return merge_chunk_lists(sorted_left_list, sorted_right_list, sort_criteria=sort_criteria)

def split_input(input_file, working_dir="/tmp", amount_of_chunks=None, amount_of_workers=None, sort_criteria="queryname"):
    total_amount_lines = count_file_lines(input_file)
    if amount_of_chunks is None:
        ## Compute optimal amount of chunks
        input_file_size = os.stat(input_file).st_size
        total, used, free = shutil.disk_usage(working_dir)
        amount_of_chunks = 64

    # Include header
    total_amount_lines -= 1
    lines_per_file = ceil(float(total_amount_lines) / float(amount_of_chunks))
    files_to_merge = []

    if amount_of_workers is None:
        amount_of_workers = os.cpu_count()

    joined_column = 11

    header = []

    with open(input_file, 'rb') as input_file, Pool(processes=amount_of_workers) as pool:
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
                files_to_merge.append((pool.apply_async(sort_sam_file, (new_chunk_file,), {"sort_criteria":sort_criteria}), new_chunk_file))
                print(files_to_merge)
                #files_to_merge.append((sort_sam_file(new_chunk_file, sort_criteria=sort_criteria), new_chunk_file))
                chunk_counter += 1
                new_chunk_file = base_output_file + str(chunk_counter) + ".sam"
                output_file = open(new_chunk_file, 'bw')
        output_file.close()
        files_to_merge.append((sort_sam_file(new_chunk_file, sort_criteria=sort_criteria), new_chunk_file))
        output_file.close()

    # We wait to all the files for now
    pool.join()
    return header, files_to_merge

def main(input_file, output_file, working_dir="/home/ramela/tmp/sam_sorter/", sort_criteria="queryname"):
    new_folder = str(uuid4())
    working_folder = working_dir + "/" + new_folder

    try:
        os.makedirs(working_folder)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    header, files_to_merge = split_input(input_file, working_dir=working_folder, sort_criteria=sort_criteria)


    #list_1 = merge_chunk_lists([files_to_merge[0]], [files_to_merge[2]], sort_criteria=sort_criteria)
    #list_2 = merge_chunk_lists([files_to_merge[1]], [files_to_merge[3]], sort_criteria=sort_criteria)
    #result_list = merge_chunk_lists(list_1, list_2, sort_criteria=sort_criteria)

    result_list = recursive_merge(files_to_merge)
    with open(output_file, 'w') as out_stream:
        for elem in result_list:
            df = pd.read_csv(elem[1], sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
            for line in df.values:
                out_stream.write('\t'.join(list(map(str, line[:-1])) + line[-1].split("#")) + "\n")

if __name__ == "__main__":
    input_file = "/home/ramela/Downloads/mergeSortSam/MergeSortSam/data/test_small.sam"
    output_file = "/home/ramela/Downloads/mergeSortSam/MergeSortSam/result/ramon_sorted_small_qn.sam"
    sort_criteria = "queryname"
    #output_file = "/home/ramela/Downloads/mergeSortSam/MergeSortSam/result/ramon_sorted_small_co.sam"
    #sort_criteria = "coordinate"
    import time
    times = []
    for i in range(3):
        start_time = time.time()
        main(input_file, output_file, sort_criteria=sort_criteria)
        times.append(time.time() - start_time)
    print("Mean time:", sum(times)/len(times))