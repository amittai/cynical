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

import os, re, shutil, subprocess, sys
from datetime import datetime

def cynical_selection(repr, avail, seed=[],
                 batch_mode=False, keep_boring=True, save_memory=True,
                 lower=True, tokenize=True, debug=True,
                 min_count=3, max_count=10000, num_lines=0,
                 outdir='/tmp', save_output=False,
                 cynical_perl_script="cynical/amittai-cynical-selection.pl"):
                 
    ''' See the large argparse section in main() for info on what these parameters actually entail. 
        In some cases I've altered the defaults in this function to be more similar to bootstrapping usecase. 
        Note that we're assuming lists of strings for our representative, available, and seed corpora.
        We're also expecting that this data is already tokenized (unless you want to run 
        selection on non-tokenized data for some weird reason.)
    '''
    
    #Set up the directory where we’re going to work (if it doesn’t exist)...
    os.makedirs(outdir, mode=0o777, exist_ok=True)
    
    #Preprocess our corpora
    if debug:
        sys.stderr.write("Sanitizing REPR Lines... at "+datetime.now().isoformat(" ")+"\n")
    repr = prep_lines(repr)
    avail = prep_lines(avail)
    if debug:
        sys.stderr.write("Sanitizing AVAIL Lines... at "+datetime.now().isoformat(" ")+"\n")
    seed = prep_lines(seed)
    if debug:
        sys.stderr.write("Sanitizing SEED Lines... at "+datetime.now().isoformat(" ")+"\n")

    #We only care about having the full corpus for the available set written to file for downstream use
    #For the others, we're just cleaning them up for use for vocabulary statistics.
    avail_corpus_file=outdir+"/avail.corpus"
    with open(avail_corpus_file, 'w') as available_corpus:
        available_corpus.write('\n'.join(avail)+'\n')

    #Get vocabulary counts for our corpora
    repr_words=get_vocab_counts(repr)
    avail_words=get_vocab_counts(avail)
    seed_words=get_vocab_counts(seed)
    
    #Save said counts to temporary files
    repr_vcb_file=outdir+"/repr.vocab"
    avail_vcb_file=outdir+"/avail.vocab"
    seed_vcb_file=outdir+"/seed.vocab"
    
    write_vocab_counts(repr_words, repr_vcb_file)
    write_vocab_counts(avail_words, avail_vcb_file)
    write_vocab_counts(seed_words, seed_vcb_file)
    
    sys.stderr.write(datetime.now().isoformat(" ")+\
    " Corpora have been preprocessed, vocab files written to disk\n")

    #Get vocabulary ratios <- probably not needed
    ratios=get_vocab_ratios(repr_words, avail_words)
    
    #Oh, and write them to disk!
    vcb_ratio_file=outdir+"/repr.avail.vocab.ratios"
    write_vocab_ratios(ratios, vcb_ratio_file)
    sys.stderr.write(datetime.now().isoformat(" ")+\
    " Vocabulary ratios calculated, written to disk\n")
    
    #Delete the things that will eat memory
    del repr_words
    del avail_words
    del seed_words
    del repr
    del seed
    
    jaded_file=outdir+"/jaded.output"

    #Now begins the process of writing up our command-line arguments for calling the thing
    #shlex says we want our calls to look like this:
    cynical_args=[cynical_perl_script, \
                  '--task_vocab='+repr_vcb_file, \
                  '--unadapt_vocab='+avail_vcb_file, \
                  '--available='+avail_corpus_file, \
                  '--seed_vocab='+seed_vcb_file, \
                  '--working_dir='+outdir, \
                  '--stats='+vcb_ratio_file, \
                  '--jaded='+jaded_file, \
                  '--mincount='+str(min_count), \
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
        sys.stderr.write("Calling cynical perl script... at "+datetime.now().isoformat(" ")+"\n")
        sys.stderr.write("\n  ".join(cynical_args)+"\n")
    with subprocess.Popen(cynical_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as cynical_proc:
        with open(outdir+"/cynical.stderr", "wb") as stderr, open(outdir+"/cynical.stdout", "wb") as stdout:
            stderr.write(cynical_proc.stderr.read())
            stdout.write(cynical_proc.stdout.read())
    #Assuming all went well above, unify our scores / line order with our original avail list
    cynical_out = read_jaded(jaded_file, avail)

    if save_output == False:
        #nuke our output files. We have scores, and I want the world to burn!
        shutil.rmtree(outdir, ignore_errors=True)

    return cynical_out

def prep_lines(corpus, lower=True):
    ''' Takes in a corpus, does the hackneyed preprocessing for downsteam perl/cynical requirements'''
    # "match at least two underscores preceded by space or
    # start-of-line, and mark them"
    corpus_out = []
    for line in corpus:
        line = re.sub(r'(\s|^)(__+)', r'\1@\2@', line) #probably not needed
        # Get rid of nbsp's and unpleasant runs of spaces
        line = re.sub(r'\s+', ' ', line)
        line = line.strip()
        if lower == True:
            line = line.lower()
        corpus_out.append(line)
    return corpus_out

def get_vocab_counts(corpus):
    ''' Reads in a list of sentences, returns a list of tuples of (word, probability, count)
    sorted by descending frequency. This is a reimplementation of 'amittai-vocab-compute-counts.pl' '''
    vocab_list = [word for line in corpus for word in line.split()]
    vocab_size = len(vocab_list)
    vocab_counts = {}
    for word in vocab_list:
        if word in vocab_counts:
            vocab_counts[word] += 1
        else:
            vocab_counts[word] = 1
    word_stats = []
    
    #We sort first by numerical score (in Descending order, hence the minus 1)
    #And then we sort alphabetically
    for word in sorted(vocab_counts, key=lambda x: (-1 * vocab_counts[x], x)):
        word_stats.append((word, vocab_counts[word]/float(vocab_size), vocab_counts[word]))
    return word_stats

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

    #First we collect the counts and probs of everything in our first (tgt)
    for tuple in vc1:
        #Variable names for readability
        word = tuple[0]
        prob1 = tuple[1]
        count1 = tuple[2]

        ratio_dict[word] = {}
        ratio_dict[word]['vc1_prob'] = prob1
        ratio_dict[word]['vc1_count'] = count1

    for tuple in vc2:
        #Variable names for readability
        word = tuple[0]
        prob2 = tuple[1]
        count2 = tuple[2]

        if word in ratio_dict: 
            ratio_dict[word]['ratio'] = ratio_dict[word]['vc1_prob'] / prob2
        else:
            #cover the case of words in distribution 2, but not in distribution 1
            ratio_dict[word] = {}
            #Set ratio to 1 over 2 * count for smoothing
            ratio_dict[word]['ratio'] = 1/(2 * count2)
            ratio_dict[word]['vc1_prob'] = 0
            ratio_dict[word]['vc1_count'] = 0

        ratio_dict[word]['vc2_prob'] = prob2
        ratio_dict[word]['vc2_count'] = count2

        for whatever in ratio_dict:
            #Cover the case of words in distribution 1, not in distribution 2
            if 'vc2_count' not in ratio_dict[whatever]:
                ratio_dict[whatever]['vc2_prob'] = 0
                ratio_dict[whatever]['vc2_count'] = 0
                ratio_dict[whatever]['ratio'] = 2 * ratio_dict[whatever]['vc1_count']
    return ratio_dict
    
def write_vocab_counts(vocab_counts, outfile):
    ''' Writes the above to a tab-seperated file'''
    with open(outfile, 'w') as vocab_out:
        for line in vocab_counts:
            vocab_out.write('\t'.join([str(x) for x in line])+'\n')

def write_vocab_ratios(vocab_ratios, outfile):
    ''' Writes the above to a tab-seperated file'''
    ## output columns are: word, prob1/prob2, prob1, count1, prob2, count2.
    with open(outfile, 'w') as ratios_out:
        #Like before, we're using -1*stuff for reverse sort, so the alphabetical stuff stays in the normal order 
        for word in sorted(vocab_ratios.items(), key=lambda x: (-1 * x[1]['ratio'], -1 * x[1]['vc1_prob'], -1 * x[1]['vc2_prob'], x[0])):
            ratios_out.write(word[0]+'\t')
            ratios_out.write(str(word[1]['ratio'])+'\t')
            ratios_out.write(str(word[1]['vc1_prob'])+'\t')
            ratios_out.write(str(word[1]['vc1_count'])+'\t')
            ratios_out.write(str(word[1]['vc2_prob'])+'\t')
            ratios_out.write(str(word[1]['vc2_count'])+'\t')
            ratios_out.write('\n')

def read_jaded(jaded_file, avail):
    ''' Takes in a file of so-called jadedness (the output of cynical), as well as the raw corpus of available lines. '''
    jaded_info = {}
    with open(jaded_file, 'r') as jaded:
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

#def main():

if __name__ == "__main__":
#    main()
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
    args = parser.parse_args()
    
    #Basic AF file IO. Sanitization follows in the prep_corpus function.
    sys.stderr.write("Reading representative lines from "+args.repr+" at "+datetime.now().isoformat(" ")+"\n")
    sys.stderr.flush()
    with open(args.repr) as repr_file:
        repr = repr_file.readlines()
        
    sys.stderr.write("Reading available lines from "+args.avail+" at "+datetime.now().isoformat(" ")+"\n")
    sys.stderr.flush()
    with open(args.avail) as avail_file:
        avail= avail_file.readlines()
        
    if args.seed:
        sys.stderr.write("Reading seed lines from "+args.seed+" at "+datetime.now().isoformat(" ")+"\n")
        sys.stderr.flush()
        with open(args.seed) as seed_file:
            seed = seed_file.readlines()
    else:
        seed = []

    cynical_out = cynical_selection(repr, avail, seed,
                 batch_mode=args.batch_mode, keep_boring=args.keep_boring, lower=args.lower,
                 min_count=args.min_count, max_count=args.max_count, num_lines=args.num_lines,
                 outdir=args.out_dir, save_memory=args.save_memory, save_output=True)
    sys.stderr.write("We've finished with our selection, output is saved to "+args.out_dir+"/jaded.output\n")


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
