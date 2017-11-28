#!/usr/bin/perl
use warnings;
use strict;
use Getopt::Long;
use open ':std', ':encoding(UTF-8)'; ## use utf8 for stdin/stdout/stderr

## original copyright amittai axelrod 2014. Released under MIT License
## (included at end).  if you port or update this code, i'd appreciate
## a copy.

## this script counts the number of occurrences of each word type in a
## file. the words are printed in decreasing count order, with each
## line having three columns: the word type, the word probability [count /
## total tokens in corpus], and its count (number of tokens).

my $corpus = "";
my $vcb_file = "";

GetOptions ("corpus=s" => \$corpus,
            "vcb_file=s" => \$vcb_file);

open(CORPUS, "<$corpus")
    or die "no such file $corpus: $!";
open(VCB, ">$vcb_file")
    or die "no such vocabulary file $vcb_file: $!";
binmode CORPUS, ':encoding(UTF-8)';
binmode VCB, ':encoding(UTF-8)';

my %words;
my $corpus_size=0;
while(<CORPUS>) {
    chomp;
    ## deal with non-breaking whitespace in UTF-8. if you leave it in,
    ## it will break all downstream uses of 'split' or 'cut', because
    ## reasons. swiped from http://www.perlmonks.org/?node_id=572690
#    tr/\xA0/ /;
    ## if you really want to change every kind of whitespace and every
    ## string of 2+ whitespaces to a single space, do this instead:
    s/\s+/ /g;  ## in utf8 strings, \s matches non-breaking space
    ## increment counter each time we see a word in file.
    foreach my $token (split(' ', $_)) {
        $words{$token}++;
        $corpus_size++;
    }
}

## sorts by word count (decreasing) and then alphabetically (increasing)
foreach ( sort { ($words{$b} <=> $words{$a}) || ($a cmp $b) } (keys %words) ) {
    my $token = $_;
    my $count=$words{$token};
    ## print the word, prob, and count
    print VCB "$token\t". sprintf("%.10f",$count/$corpus_size) ."\t$count\n";
}

close CORPUS;
close VCB;
exit;


## The MIT License (MIT)
##
## Copyright 2014-2017 amittai axelrod
## Copyright 2017      Amazon Technologies, Inc.
##
## Permission is hereby granted, free of charge, to any person
## obtaining a copy of this software and associated documentation
## files (the "Software"), to deal in the Software without
## restriction, including without limitation the rights to use, copy,
## modify, merge, publish, distribute, sublicense, and/or sell copies
## of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:
##
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
