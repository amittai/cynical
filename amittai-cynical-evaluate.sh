#!/bin/bash
set -u

## original copyright amittai axelrod 2012. Released under MIT License
## (included at end). if you port or update this code, i'd appreciate a copy.

# User specifies up to 4 files; this operates only on one side of a parallel
# corpus. Feel free to paste the halves together to do bilingual selection,
# but this only takes one set of files, not a set of parallel file pairs.

# 1. a Seed file for building the initial model (start _from_)
# 2. a Task corpus to measure our selection progress on (optimize _to_)
# 3. a Unadapted corpus for the distribution we want to optimize _from_
# 4. an Available corpus to pick new sentences _from_

# Normally "Unadapted" = "Available" and "Seed" = empty.

data_dir=/exp-efs/amittai/exp/my_data
code_path=/exp-efs/amittai/my_code

## these two are just used to compute the vocab stats that define the
## probability distributions for the language used in each corpus.
task_distribution_file="representative_sentences.en"  ## data used to define the task
unadapted_distribution_file="all_other_data.en" ## what the rest of the data looks like
## these two are corpora.
seed_corpus_file="" ## anything already translated from available_corpus
available_corpus_file=$unadapted_distribution_file  ## candidate sentence pool
## this is the output file
jaded_file=jaded.$available_corpus_file

working_dir=$data_dir
mkdir -p $working_dir
cd $working_dir

echo " * Train LMs on selected slices, evaluate on AVAIL..."
kenlm_path=/exp-efs/amittai/kenlm/build/bin
#stdoutput=$working_dir/$available_corpus_file.cynical
vocab_padsize=4000000
## here are the datapoints we want.
extra_n="100000 500000 1000000 2000000 3000000 4000000 6000000 8000000 10000000 15000000 20000000"
########
## everything that follows should run automatically
########
date
# ## make slices, compute perplexities and OOV.
# echo -n " * computing subcorpora...   "
#big_n="$(gzip -cdf $available_corpus_file | wc -l)"
#digits=$(echo -n "${#big_n}")
# JUST THIS ONCE:
digits=9
    # x=16 ## start at 2^16=65,536
    # n=$(( 2**$x ));
    # n=$(( $n/1000 * 1000 )); ## easier to read
    # ## n is powers of 2 (rounded down to nearest 1000)
n_list=''
    # while (( $n < $big_n )); do
    #     ## dynamically set the zero-padding. the "10#$foo" forces bash to
    #     ## treat zero-padded numbers as decimal, not octal (the default).
    #     n=$(printf "%0*d" $digits $((10#$n)) )
    #     n_list="${n_list}$n "
    #     ## increment $n
    #     (( x++ ))
    #     n=$(( 2**$x ));
    #     n=$(( $n/1000 * 1000 )); ## round numbers are easier to read
    # done;
echo "n_list = $n_list and $extra_n xxx"
for n in $n_list $extra_n; do
    ## dynamically set the zero-padding. the "10#$foo" forces bash to
    ## treat zero-padded numbers as decimal, not octal (the default).
    n_pad=$(printf "%0*d" $digits $((10#$n)) )
    curr_dir=$working_dir/$n_pad
    mkdir -p $curr_dir
    cd $curr_dir
    selected_file=$working_dir/$jaded_file
    if [ ! -f $selected_file ]; then
        continue;  ## no output, skip
    fi;
    slice_name=slice.$jaded_file.$n_pad
    if [ ! -f $curr_dir/$slice_name ] \
        && [ ! -f $curr_dir/$slice_name.gz ]; then
        cut -f 9 $selected_file | head -n $n > $curr_dir/$slice_name
        slice_size="$(gzip -cdf $curr_dir/$slice_name | wc -l)"
        if [ $n -ne $slice_size ]; then
            ## we are not ready for it yet.
            rm $curr_dir/$slice_name;
            continue;
        fi;
        echo "  slice size $n_pad ok"
    fi;
    slice_lm=$curr_dir/lm.$slice_name.fix.bin
    ppl_slice_lm=$curr_dir/ppl.$slice_name.fix
    ## skip if we've already got the perplexity scores
    if [ ! -f $ppl_slice_lm ] \
        && [ ! -f $ppl_slice_lm.gz ]; then
        ## train LM on the slice
        if [ ! -f $slice_lm ]; then
            ## 4gram model, temp directory here.
            $kenlm_path/lmplz  -o 4  -T . \
                -S 10% --discount_fallback \
                --vocab_pad $vocab_padsize --skip_symbols \
                < $curr_dir/$slice_name      \
                > $slice_lm.tmp
            ## binarize for efficiency
            $kenlm_path/build_binary  $slice_lm.tmp   $slice_lm
#                    rm -f $slice_lm.tmp
        fi;
        ## compute ppl on $available_corpus with lm
        echo "$n_pad" > $ppl_slice_lm
        $kenlm_path/query -v summary  $slice_lm      \
            < $working_dir/$available_corpus_file.fix \
            >> $ppl_slice_lm
    fi # if ! $ppl_slice_lm
    echo "...done"
done; # for $n
exit;


## The MIT License (MIT)
##
## Copyright 2012-2017 amittai axelrod
## Copyright 2017      Amazon Technologies, Inc.
## Copyright 2018-2019 amittai axelrod
##
## Permission is hereby granted, free of charge, to any person
## obtaining a copy of this software and associated documentation
## files (the "Software"), to deal # in the Software without
## restriction, including without limitation the rights # to use,
## copy, modify, merge, publish, distribute, sublicense, and/or sell #
## copies of the Software, and to permit persons to whom the Software
## is # furnished to do so, subject to the following conditions:
## The above copyright notice and this permission notice shall be
## included in all copies or substantial portions of the Software.
##
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
## EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
## MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
## NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
## BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
## ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
## CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
## SOFTWARE.

# eof
