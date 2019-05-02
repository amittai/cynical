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

## batchmode selects sqrt(k) sentences per iteration. much faster, much
## more approximate. essential for huge corpora, but you probably want
## to disable it when picking only the very best 10,000 sentences.
batchmode=1
## ignore words that appear fewer than $mincount times. default 3.
mincount=20
## ignore words that appear more than $maxcount times, at least until
## there are < $maxcount left in AVAIL. this affects how much memory
## is used. default 10,000. try sqrt(N), N**(2/3), or N/1000 for
## bigger relative values. 0 is default and is ignored.
maxcount=0
## how many lines to select? leave at 0 to select all.
numlines=0
## set keep_boring to 1 if you'd like to select base on words with a
## vocab ratio close to 1. this option will probably be removed.
keep_boring=1
## set to 1 to lowercase the data. helps reduce lexicon further.
needs_lowercasing=1
## TO-DO: add a $verbose var to turn off all the logging.
verbose=0

working_dir=$data_dir
mkdir -p $working_dir
cd $working_dir
## mark all tokens that start with double underscores, because we use
## __ to indicate special information later.
for file in $task_distribution_file $unadapted_distribution_file $seed_corpus_file $available_corpus_file; do
    if [ ! -f $file.fix ] && [ ! -f $file.fix.gz ]; then
        ## "match at least two underscores preceded by space or
        ## start-of-line, and mark them"
        ## also need to nuke non-breaking whitespace:
        ## s/\s+/ /g;  ## in utf8 strings, \s matches non-breaking space
        ## perl -CS : declare STD{IN,OUT,ERR} to be UTF-8
	lowercasing=''
	if [ "$needs_lowercasing" -eq "1" ]; then
	    lowercasing='$_=lc;'
	fi
	## what a mess. In order to have the lowercasing optional, we build up a command in parts.
	command1=' | perl -pe '\''s/(\s|^)(__+)/$1\@$2\@/g;'\'''
	command2=' | perl -CS -pe '\''s/\s+/ /g; s/ $//; $_.="\n"; ' ## yes, it's unbalanced
        eval cat $file $command1 $command2 $lowercasing "'" > $file.fix
    fi;
done;

## compute vocab stats for each distribution file
for file in $task_distribution_file $unadapted_distribution_file $seed_corpus_file $available_corpus_file; do
    echo -n " * compute vocab stats for $file ...   "
    input=$data_dir/$file.fix
    output=$working_dir/vocab.$file.fix
    input_tmp=$data_dir/$file.tmp
    if [ ! -f $output ] && [ ! -f $output.gz ]; then
	if [ "$needs_lowercasing" -eq "1" ]; then
  	    ## perl -CS : declare STD{IN,OUT,ERR} to be UTF-8
	    ## see http://perldoc.perl.org/perlrun.html#*-C-[_number/list_]*
	    cat $input | perl -CS -pe '$_=lc($_);' > $input_tmp
	else
	    ln -s $input $input_tmp
	fi

	$code_path/amittai-vocab-compute-counts.pl \
	    --corpus=$input_tmp                     \
	    --vcb_file=$output
	rm $input_tmp
    fi
    echo "...done"
done;
## compute relative vocab stats for the corpora
echo -n " * compute relative statistics between corpora ...   "
output=$working_dir/vocab.ratios.task-unadapted
if [ ! -f $output ] && [ ! -f $output.gz ]; then
    $code_path/amittai-vocab-ratios.pl \
	--model1=$working_dir/vocab.$task_distribution_file.fix     \
	--model2=$working_dir/vocab.$unadapted_distribution_file.fix \
	| sort --general-numeric-sort --reverse --key=2,2             \
	> $output
fi
echo "...done"

input=$output
echo " * tmp_message: calling perl script ...   "
## stdout/stderr contain useful info for debugging. the actual
## selected data appears in the $jaded file.
stdoutput=$working_dir/$available_corpus_file.cynical
output=$working_dir/$jaded_file
flags=" --mincount=$mincount ";
if [ "$batchmode" -gt "0" ]; then
    ## set batchmode flag
    flags="${flags} --batchmode "
fi
if [ "$keep_boring" -gt "0" ]; then
    ## set batchmode flag
    flags="${flags} --keep_boring "
fi
if [ "$maxcount" -gt "0" ]; then
    ## set maxcount.
    flags="${flags} --maxcount=$maxcount "
fi
if [ "$numlines" -gt "0" ]; then
    ## set numlines.
    flags="${flags} --numlines=$numlines "
fi
if [ ! -f $working_dir/$jaded_file ] && [ ! -f $working_dir/$jaded_file.gz ]; then
    $code_path/amittai-cynical-selection.pl                  \
	--task_vocab=$data_dir/vocab.$task_distribution_file.fix          \
	--unadapt_vocab=$data_dir/vocab.$unadapted_distribution_file.fix \
	--available=$data_dir/$available_corpus_file.fix        \
	--seed_vocab=$working_dir/vocab.$seed_corpus_file.fix    \
	--working_dir=$working_dir \
	--stats=$input              \
	--jaded=$output     $flags   \
	> $stdoutput.stdout  2> $stdoutput.stderr
fi
echo "...done"
## Note that the jaded.*.txt file will contain double_underscores marked as "@__@".

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
