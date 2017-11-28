#!/usr/bin/perl
use warnings;
use strict;
use Getopt::Long;
use open ':std', ':encoding(UTF-8)'; ## use utf8 for stdin/stdout/stderr

## original copyright amittai axelrod 2014. Released under MIT License (included at end).
## if you port or update this code, i'd appreciate a copy.

## Takes two files, each with three tab-separated columns, produced by
## amittai-compute-vocab-counts.pl; each line consists of a words, a
## probability, and the count of the word in a corpus. Each file is
## thus a unigram language model with count information. Script merges
## the information and write tab-separated output to STDOUT. The
## output columns are: word, prob1/prob2, prob1, count1, prob2,
## count2. Usually used with model1=in-domain task and model2= general
## data, making the ratio prob1/prob2 "number of times more likely the
## word is in the in-domain corpus than in the general corpus".

## If a word is only in one corpus, than the ratio is twice what it
## would have been if the word had appeared once in the other
## corpus. that is, we smooth the count in the other corpus to be 0.5.
## No real reason, just seems like we shouldn't ignore words that
## appear lots in one corpus but not at all in the other, and it
## should be more skewed than if it had been a singleton.

my $model1 = "";
my $model2 = "";

GetOptions ("model1=s" => \$model1,
            "model2=s" => \$model2);
open(MODEL1, "<$model1") or die "no such model1 file $model1: $!";
open(MODEL2, "<$model2") or die "no such model2 file $model2: $!";
binmode MODEL1, ':encoding(UTF-8)';
binmode MODEL2, ':encoding(UTF-8)';

my %words1;
while(<MODEL1>) {
    chomp;
    ## read in triples {word, prob, count} in model1
    my @line1 = split("\t",$_);
    $words1{$line1[0]}{prob} = sprintf("%.10f",$line1[1]);
    $words1{$line1[0]}{count} = $line1[2];
}

while(<MODEL2>) {
    chomp;
    ## model2 contains same triples {word, prob, count}
    my @line2 = split("\t", $_);
    my $word = $line2[0];
    ## first the word itself
    my @output = ($word);
    ## compute the delta of the probabilities; write model1 data.
    ## if word $line2[1] was also in Model 1, produce the ratio of
    ## probs Model1/Model2. else just say... 1/(2*count_model2)?.
    ## delta column should be the change _from_ model 1 score, because
    ## model1 is the in-domain (so delta * model 2 = model 1).
    if (defined $words1{$word}{count}) {
	@output = (@output, 
		   sprintf("%.10f", ($words1{$word}{prob} / $line2[1])),
		   $words1{$word}{prob},
		   $words1{$word}{count});
    } else {
	@output = (@output, 
		   sprintf("%.10f", (0.5 / $line2[2])),
		   0,
		   0);
    }
    ## now add the model2 probability and count
    @output = (@output, sprintf("%.10f",$line2[1]), $line2[2]);
    ## print out all columns for the word (this prints out all words
    ## in model1 over the course of the outer loop)
    if (defined $line2[2]) {
	print STDOUT join("\t", @output) . "\n";
    }
    ## remove the word from model1
    delete $words1{$word};
}

## any remaining elements of Model1 aren't in Model2. Set to
## 2*count_model1.
foreach my $word (keys %words1) {
    # set ratio to 0, produce the model1 probability and count, set model2 to zero.
    my @output = ($word,
		  2*$words1{$word}{count},
		  sprintf("%.10f",$words1{$word}{prob}),
		  $words1{$word}{count},
		  0, 0); 
    print STDOUT join("\t", @output) . "\n";
}

close MODEL1;
close MODEL2;
exit;


## The MIT License (MIT)
##
## Copyright 2014-2017 amittai axelrod
## Copyright 2017      Amazon Technologies, Inc.
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
