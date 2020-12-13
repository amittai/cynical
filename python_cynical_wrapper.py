#!/usr/bin/env python3

'''
This is a Python re-implementation of 
amittai-cynical-wrapper.sh
amittai-vocab-compute-counts.pl
amittai-vocab-ratios.pl
from https://github.com/amittai/cynical

It remains dependant on the original
amittai-cynical-selection.pl

But, it allows the user to run cynical data
selection as a python module

The original work is by amittai axelrod
The port is by Steve Sloto
'''

import os
import re
import shutil
import subprocess
import sys
from collections import Counter
import datetime
import copy

def cynical_selection(repr_lines, avail_lines, seed_lines=[],
                 batch_mode=False, keep_boring=True, 
                 save_memory=True, lower=True, debug=True,
                 min_count=3, max_count=10000, num_lines=0,
                 outdir='/tmp/cynical_out', save_output=False,
                 cynical_perl_script="amittai-cynical-selection.pl"):
                 
    ''' See the large argparse section in main() for info on what these parameters actually entail. 
        In some cases I've altered the defaults in this function to be more similar to bootstrapping usecase. 
        Note that we're assuming lists of strings for our representative, available, and seed corpora.
        We're also expecting that this data is already tokenized (unless you want to run 
        selection on non-tokenized data for some weird reason.)
    '''

    #Check that the cyncial_perl_script exists and can be executed.

# Uncomment if you're using this as a pointer to a local script rather than something on the shell path
#    if not os.path.exists(cynical_perl_script):
#        raise FileNotFoundError("Path for cynical_perl_script does not exist!")

    if shutil.which(cynical_perl_script) == None:
        raise PermissionError("cynical_perl_script is not  executible...")

    # Set up the directory where we’re going to work (if it doesn’t exist)...
    os.makedirs(outdir, mode=0o777, exist_ok=True)
    
    # Run a few checks up front to see if any of our inputs are equivalent.
    # If so, we can cut down on our pre-processing.
    if debug:
        sys.stderr.write("Checking whether input files are duplicates at {}\n".format(get_timestamp()))
    avail_is_repr = (avail_lines == repr_lines)
    seed_is_repr = (seed_lines == repr_lines)
    seed_is_avail = (seed_lines == avail_lines)

    # Cool, now we can change the contents of the lists away from how they were originally input.
    # We'll start with avail, and follow all of the steps needed to get it ready for the perl script.
    if debug:
        sys.stderr.write("Sanitizing AVAIL Lines... at {}\n".format(get_timestamp()))
    # cast this to a list b.c. we want to use its output twice (once to save for cynical, once for calculating mad stats)
    avail_lines = list(prep_lines(avail_lines))
    
    # We only care about having the full corpus for the available set written to file for downstream use
    # For the others, we're just cleaning them up for use for vocabulary statistics.
    sys.stderr.write("Writing preprocessed AVAIL corpus to disk at {}\n".format(get_timestamp()))
    avail_corpus_file = os.path.join(outdir, "avail.corpus")
    with open(avail_corpus_file, 'w', encoding='utf-8') as available_corpus:
        for line in avail_lines:
            available_corpus.write(line)
            available_corpus.write('\n')
    
    # Now we'll get our vocabulary counts, and write those to disk
    if debug:
        sys.stderr.write("Counting AVAIL Words... at {}\n".format(get_timestamp()))
    avail_counts = get_vocab_counts(avail_lines)
    
    if avail_is_repr:
        # if we've prepped the same list already, skip the computation.
        repr_counts = copy.deepcopy(avail_counts)
        repr_vcb_file = avail_vcb_file
    else:
        # if we have a separate representative set, do the same prep minus writing the corpus to disk
        if debug:
            sys.stderr.write("Sanitizing REPR Lines... at {}\n".format(get_timestamp()))
        repr_lines = prep_lines(repr_lines)

        if debug:
            sys.stderr.write("Counting REPR Words... at {}\n".format(get_timestamp()))
        repr_counts = get_vocab_counts(repr_lines)

    # Similar checks for whether we run computation for seed
    if seed_is_repr:
        seed_counts = copy.deepcopy(repr_counts)
    elif seed_is_avail:
        seed_counts = copy.deepcopy(avail_counts)
    else:
        # If we have a unique seed, we do our preprocessing and counting.
        if debug:
            sys.stderr.write("Sanitizing & Counting SEED Lines... at {}\n".format(get_timestamp()))
        seed_lines = prep_lines(seed_lines)
        seed_counts = get_vocab_counts(seed_lines)

    # Since we don't care about our seed counts for any ratios, we output them now.
    if debug:
        sys.stderr.write("Writing SEED Words... at {}\n".format(get_timestamp()))

    seed_vcb_file = os.path.join(outdir, "seed.vocab")
    with open(seed_vcb_file, 'w', encoding='utf-8') as seed_vcb_out:
        for line in seed_counts:
            seed_vcb_out.write("\t".join([str(x) for x in line]))
            seed_vcb_out.write("\n")

    if debug:
        sys.stderr.write("{} -- All Corpora have been preprocessed, vocab files written to disk\n".format(get_timestamp()))

    sys.stderr.write("{} Calculating vocabulary ratio...\n".format(get_timestamp()))
    ratios = get_vocab_ratios(repr_counts, avail_counts)

    # We output vocabulary ratios, along with counts for REPR & AVAIL simultaneously...
    sys.stderr.write("{}  -- Writing vocabulary ratios & avail/repr counts to disk...\n".format(get_timestamp()))
    vcb_ratio_file = os.path.join(outdir, "repr.avail.vocab.ratios")
    repr_vcb_file = os.path.join(outdir, "repr.vocab")
    avail_vcb_file = os.path.join(outdir, "avail.vocab")

    with open(vcb_ratio_file, 'w', encoding='utf-8') as ratios_out, \
            open(repr_vcb_file, 'w', encoding='utf-8') as repr_counts_out, \
            open(avail_vcb_file, 'w', encoding='utf-8') as avail_counts_out:
        for word_name, word_stats in ratios.items():
            # count file formats are word <tab> probability <tab> count
            if word_stats.corpus_1_count is not None:
                repr_counts_out.write('{}\t{}\t{}\n'.format(word_name, str(word_stats.corpus_1_probability), str(word_stats.corpus_1_count)))
            if word_stats.corpus_2_count is not None:
                avail_counts_out.write('{}\t{}\t{}\n'.format(word_name, str(word_stats.corpus_2_probability), str(word_stats.corpus_2_count)))

            # ratio file is also a *.tsv, order of columns besides word_name is provided in get_value_list method of WordProbabilityInformation
            ratios_out.write(word_name)
            ratios_out.write('\t')
            ratios_out.write('\t'.join([str(value) for value in word_stats.get_value_list()]))
            ratios_out.write('\n')

    #Delete the things that will eat memory
    del repr_counts
    del avail_counts
    del seed_counts
    del repr_lines
    del seed_lines
    
    jaded_file = os.path.join(outdir, "jaded.output")

    #Now begins the process of writing up our command-line arguments for calling the thing
    #shlex says we want our calls to look like this:
    cynical_args=[cynical_perl_script,
                  '--task_vocab='+repr_vcb_file,
                  '--unadapt_vocab='+avail_vcb_file,
                  '--available='+avail_corpus_file,
                  '--seed_vocab='+seed_vcb_file,
                  '--working_dir='+outdir,
                  '--stats='+vcb_ratio_file,
                  '--jaded='+jaded_file,
                  '--mincount='+str(min_count),
                      ]

    if num_lines > 0:
        cynical_args.append('--numlines='+str(num_lines))
    if max_count > 0:
        cynical_args.append('--maxcount='+str(max_count))
    if batch_mode == True: 
        cynical_args.append('--batchmode')
    if keep_boring == True: 
        cynical_args.append('--keep_boring')
    if save_memory == True:
        cynical_args.append('--save_memory')
            
    #Call our perl script
    if debug:
        sys.stderr.write("Calling cynical perl script... at {}\n".format(get_timestamp()))
        sys.stderr.write("\n  ".join(cynical_args)+"\n")
    with subprocess.Popen(cynical_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as cynical_proc:
        with open(outdir+"/cynical.stderr", "wb") as stderr, open(outdir+"/cynical.stdout", "wb") as stdout:
            stderr.write(cynical_proc.stderr.read())
            stdout.write(cynical_proc.stdout.read())
    #Assuming all went well above, unify our scores / line order with our original avail list
    cynical_out = read_jaded(jaded_file, avail_lines)

    if save_output == False:
        #nuke our output files. We have scores, and I want the world to burn!
        shutil.rmtree(outdir, ignore_errors=True)
    else:
        if debug:
            sys.stderr.write("We've finished with our selection, output is saved to "+jaded_file+"\n")
    return cynical_out

def get_timestamp():
    return datetime.datetime.now().isoformat(" ")

none_to_zero = lambda x: 0 if x is None else x

class WordProbabilityInformation():
    '''Utility object for holding corpus-level stats about word probabilities'''
    def __init__(self):
        self.corpus_1_probability = None
        self.corpus_1_count = None
        self.corpus_2_probability = None
        self.corpus_2_count = None
        self.ratio = None

    def add_corpus_1_info(self, probability, count):
        self.corpus_1_probability = probability
        self.corpus_1_count = count

    def add_corpus_2_info(self, probability, count):
        self.corpus_2_probability = probability
        self.corpus_2_count = count

    def compute_ratio(self):
        # if we have probabilities for corpus_1 and corpus_2, then we ratio those
        if self.corpus_1_probability is not None and self.corpus_2_probability is not None:
            self.ratio = self.corpus_1_probability / self.corpus_2_probability
        # if we have probabilities for only corpus 2
        elif self.corpus_2_count is not None:
            self.ratio = 1/(2 * self.corpus_2_count)
        # if we have probabilities for only corpus 1
        elif self.corpus_1_count is not None:
            self.ratio = 2 * self.corpus_1_count

    def get_value_list(self):
        '''Returns cleaned list of values in format used by vocab ratio file.'''
        return [self.ratio,
                  none_to_zero(self.corpus_1_probability),
                  none_to_zero(self.corpus_1_count),
                  none_to_zero(self.corpus_2_probability),
                  none_to_zero(self.corpus_2_count)]


def prep_lines(corpus, lower=True):
    ''' Takes in a corpus (list of str), does the hackneyed preprocessing for downsteam perl/cynical requirements'''

    # "match at least two underscores preceded by space or start-of-line, and mark them"
    corpus = map(lambda x: re.sub(r'(\s|^)(__+)', r'\1@\2@', x), corpus)

    # Get rid of non-breaking space and unpleasant runs of spaces
    corpus = map(lambda x: re.sub(r'\s+', ' ', x), corpus)

    # strip other whitespace
    corpus = map(lambda x: x.strip(), corpus)

    # optional lowercase
    if lower:
        corpus = map(lambda x: x.lower(), corpus)

    return corpus


def get_vocab_counts(corpus):
    ''' Reads in a list of sentences, returns a list of tuples of (word, probability, count)
    sorted by descending frequency. This is a reimplementation of 'amittai-vocab-compute-counts.pl' '''
    vocab_counts = Counter()
    for line in corpus:
        for word in line.split():
            vocab_counts[word] += 1

    vocab_size = sum(vocab_counts.values())

    # We are /nearly there/ but we want to output words, probabilities, and counts rather than words and counts
    reformat_line = lambda wordcount: (wordcount[0], float(wordcount[1]) / vocab_size, wordcount[1])
    return map(reformat_line, vocab_counts.most_common())


def get_vocab_ratios(vc1, vc2):
    ''' Roughly speaking, this is a reimplemntation of 'amittai-vocab-ratios.pl'

    To summarize/plagarize Amittai: 
    Takes two <lists of tuples>, produced by <get_vocab_counts>
    each item consists of a words, a probability, and the count of the word in a corpus. Each file is
    thus a unigram language model with count information. <Function>  merges
    the information <together into a dictionary of dictionaries that can be writ for downstream use.>
    
    Usually used with model1=in-domain task and model2= general
    data, making the ratio prob1/prob2 "number of times more likely the
    word is in the in-domain corpus than in the general corpus".

    If a word is only in one corpus, than the ratio is twice what it
    would have been if the word had appeared once in the other
    corpus. that is, we smooth the count in the other corpus to be 0.5.
    No real reason, just seems like we shouldn't ignore words that
    appear lots in one corpus but not at all in the other, and it
    should be more skewed than if it had been a singleton.'''

    ratio_dict = {}

    # compile word stats from first list
    for word, prob1, count1 in vc1:
        # Add probability dict entries for any words that we have...
        ratio_dict[word] = WordProbabilityInformation()
        ratio_dict[word].add_corpus_1_info(prob1, count1)

    # compile word stats from second list
    for word, prob2, count2 in vc2:
        if word not in ratio_dict:
            ratio_dict[word] = WordProbabilityInformation()
        ratio_dict[word].add_corpus_2_info(prob2, count2)

    # get ratio information for compiled words
    for word_probability_information in ratio_dict.values():
        word_probability_information.compute_ratio()

    return ratio_dict


def read_jaded(jaded_file, avail):
    ''' Takes in a file of so-called jadedness (the output of cynical), as well as the raw corpus of available lines. '''
    jaded_info = {}
    with open(jaded_file, 'r', encoding='utf-8') as jaded:
        for line in jaded:
            ## JADED columns: sentence {input line_id, output rank, score,
            ## penalty, gain, total score, the root word, WGE, and the string.
            line=line.split('\t')
            raw_line = avail[int(line[0])-1] #original line number            
            jaded_info[raw_line] = {}
            jaded_info[raw_line]['input_id'] = line[0]
            jaded_info[raw_line]['output_rank'] = line[1]
            #output_rank is the same as the order the lines were selected
            jaded_info[raw_line]['score'] = line[2]
            jaded_info[raw_line]['penalty'] = line[3]
            jaded_info[raw_line]['gain'] = line[4]
            jaded_info[raw_line]['total_score'] = line[5]
            jaded_info[raw_line]['root_word']=line[6]
            jaded_info[raw_line]['WGE']=line[7]
        return jaded_info


def main():
    ''' An example implementation of reading in files for cynical from disk and calling the core perl module 
        This should function as 'amittai-cynical-wrapper.sh' except with cmd line args
        in place of a wrapper '''
        
    import argparse
    parser = argparse.ArgumentParser(description='Substitute for Amittai Bash Wrapper')
    parser.add_argument('--repr', required=True, help="Representative Set -- The Data We're Modelling")
    parser.add_argument('--avail', required=True, help="Available Set -- The Data We're Selecting From")
    parser.add_argument('--seed', help="Seed Set -- Data We Have Already Selected/Used (Optional)")
    parser.add_argument('--batch-mode', \
                            help="""batchmode selects sqrt(k) sentences per iteration. much faster, much
                                  more approximate. essential for huge corpora, but you probably want
                                  to disable it when picking only the very best 10,000 sentences.""", action='store_true', default=False)
    parser.add_argument('--min-count', help="ignore words that appear fewer than $mincount times. default 3", type=int, default=3)
    parser.add_argument('--max-count', help="""ignore words that appear more than $maxcount times, at least until
                                               there are < $maxcount left in AVAIL. this affects how much memory
                                               is used. default 10,000. try sqrt(N), N**(2/3), or N/1000 for
                                               bigger relative values.""", type=int, default=0) #downstream, this 0 is interpreted as 10,000
    parser.add_argument('--num-lines', help="how many lines to select? Not setting this parameter means every line will be selected.", type=int, default=0)
    parser.add_argument('--keep-boring', help="Use this option to select based on words with a vocab ratio close to 1.", action="store_true", default=True)
    parser.add_argument('--lower', help="Lowercase the data, and reduce vocabulary size further.", action="store_true", default=False)
    parser.add_argument('--out-dir', help='Directory to write our output files to.', required=True)
    parser.add_argument('--save-memory', help="Saves memory, runs slower", action="store_true", default="False")
    parser.add_argument('--cyn-path', help="Location of cynical perl script.", default="amittai-cynical-selection.pl")
    args = parser.parse_args()
    
    # Give the cyncial selection function file objects so that we don't read whole corpora into memory
    sys.stderr.write("Opening files at {}".format(get_timestamp()))
    with open(args.repr, 'r', encoding='utf-8') as repr_file, open(args.repr, 'r', encoding='utf-8') as avail_file:
        if args.seed:
            seed_file = open(args.seed, 'r', encoding='utf-8')
        else:
            seed_file = []

        cynical_selection(repr_file, avail_file, seed_file,
                          batch_mode=args.batch_mode, keep_boring=args.keep_boring,
                          lower=args.lower, min_count=args.min_count, max_count=args.max_count,
                          num_lines=args.num_lines,outdir=args.out_dir, save_memory=args.save_memory,
                          cynical_perl_script=args.cyn_path, save_output=True)

        # close our seed since we're done with it
        if seed_file != []:
            seed_file.close()
                 
if __name__ == "__main__":
    main()



## The MIT License (MIT)
##
## Copyright 2018     
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
