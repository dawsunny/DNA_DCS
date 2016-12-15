#!/bin/bash
echo "transfer compress"
ALL_FILES="./compress/*.cpp"
ALL_FILES+=" ./include/dc_c_*.h"
sed -i 's/dc_c_print_usage/print_usage/g' $ALL_FILES
sed -i 's/dc_c_check_arg/check_arg/g' $ALL_FILES
sed -i 's/dc_c_read_ref_file/read_ref_file/g' $ALL_FILES
sed -i 's/dc_c_free_memory/free_memory/g' $ALL_FILES
sed -i 's/dc_c_ref_seqs_g/ref_seqs_g/g' $ALL_FILES
sed -i 's/dc_c_ref_seqs_len_g/ref_seqs_len_g/g' $ALL_FILES
sed -i 's/dc_c_DIF_RATE/DIF_RATE/g' $ALL_FILES
sed -i 's/dc_c_OVERLAP/OVERLAP/g' $ALL_FILES
sed -i 's/dc_c_ref_seq_total_no_g/ref_seq_total_no_g/g' $ALL_FILES

sed -i 's/print_usage/dc_c_print_usage/g' $ALL_FILES
sed -i 's/check_arg/dc_c_check_arg/g' $ALL_FILES
sed -i 's/read_ref_file/dc_c_read_ref_file/g' $ALL_FILES
sed -i 's/free_memory/dc_c_free_memory/g' $ALL_FILES
sed -i 's/ref_seqs_g/dc_c_ref_seqs_g/g' $ALL_FILES
sed -i 's/ref_seqs_len_g/dc_c_ref_seqs_len_g/g' $ALL_FILES
sed -i 's/DIF_RATE/dc_c_DIF_RATE/g' $ALL_FILES
sed -i 's/OVERLAP/dc_c_OVERLAP/g' $ALL_FILES
sed -i 's/ref_seq_total_no_g/dc_c_ref_seq_total_no_g/g' $ALL_FILES
echo "done."

echo "transfer decompress"
ALL_FILES="./decompress/*.cpp"
ALL_FILES+=" ./include/dc_d_*.h"
sed -i 's/dc_d_print_usage/print_usage/g' $ALL_FILES
sed -i 's/dc_d_check_arg/check_arg/g' $ALL_FILES
sed -i 's/dc_d_read_ref_file/read_ref_file/g' $ALL_FILES
sed -i 's/dc_d_free_memory/free_memory/g' $ALL_FILES
sed -i 's/dc_d_ref_seqs_g/ref_seqs_g/g' $ALL_FILES
sed -i 's/dc_d_ref_seqs_len_g/ref_seqs_len_g/g' $ALL_FILES
sed -i 's/dc_d_DIF_RATE/DIF_RATE/g' $ALL_FILES
sed -i 's/dc_d_OVERLAP/OVERLAP/g' $ALL_FILES
sed -i 's/dc_d_ref_seq_total_no_g/ref_seq_total_no_g/g' $ALL_FILES

sed -i 's/print_usage/dc_d_print_usage/g' $ALL_FILES
sed -i 's/check_arg/dc_d_check_arg/g' $ALL_FILES
sed -i 's/read_ref_file/dc_d_read_ref_file/g' $ALL_FILES
sed -i 's/free_memory/dc_d_free_memory/g' $ALL_FILES
sed -i 's/ref_seqs_g/dc_d_ref_seqs_g/g' $ALL_FILES
sed -i 's/ref_seqs_len_g/dc_d_ref_seqs_len_g/g' $ALL_FILES
sed -i 's/DIF_RATE/dc_d_DIF_RATE/g' $ALL_FILES
sed -i 's/OVERLAP/dc_d_OVERLAP/g' $ALL_FILES
sed -i 's/ref_seq_total_no_g/dc_d_ref_seq_total_no_g/g' $ALL_FILES
echo "done."
