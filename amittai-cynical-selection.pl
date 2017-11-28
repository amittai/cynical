#!/usr/bin/perl
use warnings;
use strict;
use Getopt::Long;
#use Data::Dumper; ## only useful for debugging
use Benchmark qw(:all);
use Time::HiRes qw( gettimeofday );  ## to get timing info for debugging
use open ':std', ':encoding(UTF-8)'; ## use utf8 for stdin/stdout/stderr
## uncomment this 2017.11.17
#use List::BinarySearch qw( binsearch  binsearch_pos  binsearch_range );
use FileHandle;
use POSIX;
use sort '_mergesort'; ## take advantage of mostly-ordered arrays
no sort 'stable'; ## don't care if stable; speed matters more
#use Perl::Critic;
#to think about: https://metacpan.org/pod/DBM::Deep ?

## original copyright amittai axelrod 2012. Released under MIT License
## (included at end).  if you port or update this code, i'd appreciate
## a copy.

STDERR->autoflush(1); ## make STDERR 'hot' (no buffering); good for debugging
STDOUT->autoflush(1); ## make STDOUT 'hot' (no buffering); good for debugging

sub compute_sentence_gain_estimate;
sub update_length_penalties;

# task_distribution_file=       ## adaptation target
# unadapted_distribution_file=  ## un-adapted corpus
# seed_corpus_file=             ## if any
# available_corpus_file=        ## the pool of candidate sentences to pick from

## take in two corpora with the distributions of interest, a corpus of
## sentences to choose from, the vocabulary statistics of a seed corpus (if
## any), and a file containing the relative vocabulary statistics of the two
## distributions. plus the optional $batchmode and $keep_boring flags.
# $code_path/cynical-selection-amzn.pl                       \
#            --task=$data_dir/$task_distribution_file         \
#            --unadapted=$data_dir/$unadapted_distribution_file\
#            --available=$data_dir/$available_corpus_file       \
#            --seed_vocab=$working_dir/vocab.$seed_corpus_file   \
#            --working_dir=$working_dir                           \
#            --stats=$input     ( --batchmode  --keep_boring )

my ($task,$unadapted,$available,$seed_vocab,$stats,$working_dir,$jaded,$mincount,$keep_boring,$batchmode) = ('','','','','','','','','','');

GetOptions ("task=s"        => \$task,
	    "unadapted=s"   => \$unadapted,
	    "available=s"   => \$available,
	    "seed_vocab=s"  => \$seed_vocab,
	    "stats=s"       => \$stats,
	    "working_dir=s" => \$working_dir,
	    "jaded=s"       => \$jaded,
	    "mincount=s"    => \$mincount,
	    "keep_boring"   => \$keep_boring,
	    "batchmode"     => \$batchmode);

$working_dir = "." if ($working_dir eq "");
$mincount = 3 if ($mincount eq "");
(my $task_file = $task) =~ s/^.*\/(.*?)$/$1/; ## strip path
(my $available_file = $available) =~ s/^.*\/(.*?)$/$1/; ## strip path
my $task_file_squish = "$working_dir/$task_file.squish";
my $exists_task_file_squish = (-e $task_file_squish) || "";
my $exists_seed_vocab = (-e $seed_vocab) || "";
my $available_file_squish = "$working_dir/$available_file.squish";
my $exists_available_file_squish = (-e $available_file_squish) || "";

open (JADED, ">$jaded") or die "$!";
binmode JADED, ':encoding(UTF-8)';
JADED->autoflush(1); ## no buffering?

## a note on the STATS file produced by amittai-vocab-ratios.pl
## The output columns are: word, prob1/prob2, prob1, count1, prob2, count2.
## Usually used with model1=in-domain task and model2= general data, making the
## ratio prob1/prob2 "number of times more likely the word is in the in-domain
## corpus than in the general corpus".
my %lexicon;
my $task_tokens = 0;
my $unadapted_tokens = 0;
my $seed_tokens = 0;
## read in the lexicon
print STDERR " * going through the task/unadapted distribution vocabularies...   ";
open (STATS, "<$stats") or die "no such file $stats: $!";
binmode STATS, ':encoding(UTF-8)';
while(<STATS>){
    chomp;
    next if ($_ =~ m/^$/);
    my ($word,$ratio,$prob1,$count1,$prob2,$count2) = split(' ', $_);
    ## these are based on the task and unadapted distributions
    $lexicon{$word}{ratio}=$ratio;
    $lexicon{$word}{task}{count}=$count1;
    $lexicon{$word}{unadapt}{count}=$count2;
    $lexicon{$word}{task}{prob}=$prob1;
    $lexicon{$word}{unadapt}{prob}=$prob2;
    $task_tokens += $count1;
    $unadapted_tokens += $count2;
    ## these are computed from the "seed" and "available" corpora
    $lexicon{$word}{seed}{count}=0;
    $lexicon{$word}{avail}{count}=0;
}
close STATS;
print STDERR "...done\n";
if ($exists_seed_vocab) {
    print STDERR " * going through the seed vocabulary...   ";
    open (SEED_VOCAB, "<$seed_vocab") or die "no such file $seed_vocab: $!";
    binmode SEED_VOCAB, ':encoding(UTF-8)';
    while(<SEED_VOCAB>){
    	chomp;
    	next if ($_ =~ m/^$/);
    	my ($word,$prob,$count) = split(' ', $_);
    	$lexicon{$word}{seed}{count}=$count;
    	$seed_tokens += $count;
    	## add default values as needed
    	$lexicon{$word}{task}{count}=0 unless ($lexicon{$word}{task}{count});
    	$lexicon{$word}{unadapt}{count}=0 unless ($lexicon{$word}{unadapt}{count});
    	$lexicon{$word}{avail}{count}=0 unless ($lexicon{$word}{avail}{count});
    } # while
    close SEED_VOCAB;
    print STDERR "...done\n";
} # if
print STDERR " * going through the vocabulary of the available lines...   ";
open (AVAILABLE, "<$available") or die "no such file $available: $!";
binmode AVAILABLE, ':encoding(UTF-8)';
while(<AVAILABLE>){
    chomp;
    next if ($_ =~ m/^$/);
    foreach my $word (split(' ', $_)) {
    	$lexicon{$word}{avail}{count}++;
    	## add default values as needed
    	$lexicon{$word}{task}{count}=0 unless ($lexicon{$word}{task}{count});
    	$lexicon{$word}{seed}{count}=0 unless ($lexicon{$word}{seed}{count});
    	$lexicon{$word}{unadapt}{count}=0 unless ($lexicon{$word}{unadapt}{count});
    }
} # while
close AVAILABLE;
print STDERR "...done\n";
## at this point the %lexicon hash should contain all the words in
## task+unadapted vocabularies and the seed + available corpora.

## words in $task but not in $seed+$available can't be modeled
$lexicon{"__impossible"}{task}{count} = 0;
$lexicon{"__impossible"}{unadapt}{count} = 0;
$lexicon{"__impossible"}{seed}{count} = 0;
$lexicon{"__impossible"}{avail}{count} = 0;
## words in $seed+$available but not in $task can't help to model $task
$lexicon{"__useless"}{task}{count} = 0;
$lexicon{"__useless"}{unadapt}{count} = 0;
$lexicon{"__useless"}{seed}{count} = 0;
$lexicon{"__useless"}{avail}{count} = 0;
## words whose counts are too low in $task and $unadapt to provide good stats
$lexicon{"__dubious"}{task}{count} = 0;
$lexicon{"__dubious"}{unadapt}{count} = 0;
$lexicon{"__dubious"}{seed}{count} = 0;
$lexicon{"__dubious"}{avail}{count} = 0;

## squish the lexicon
my %replace; ## dictionary of words --> substitutions
my %currmodel; ## the model we update as we go (initialized with SEED)
my $currmodel_score; ## entropy of the task set using our model
print STDERR " * going though the lexicon...   ";
## have to use foreach and not 'each' because we're modifying the hash
## inside the loop.
foreach (keys %lexicon) {
    my $word=$_;
    next if ($word =~ m/^__/); ## don't mess with the labels we're creating
    ## constant part of task perplexity computation: [ -C_task(v)/C(task) ]
    ## recall we put total task size in $task_tokens
    if (0 == $lexicon{$word}{task}{count}) {
        ## replace useless words (task count = 0, so we don't care about it)
        $replace{$word} = "__useless";
        ## the count in task is 0 by construction
        $lexicon{"__useless"}{unadapt}{count} += $lexicon{$word}{unadapt}{count};
        $lexicon{"__useless"}{seed}{count}  += $lexicon{$word}{seed}{count};
        $lexicon{"__useless"}{avail}{count} += $lexicon{$word}{avail}{count};
        $currmodel{"__useless"}{count} += $lexicon{$word}{seed}{count};
        delete $lexicon{$word};
    } elsif (0 == $lexicon{$word}{avail}{count}) {
	## these words will have 0 candidate sentences affiliated with
	## them. regardless of what's in the seed, we can't do
	## anything about them. remove to avoid having empty arrays.
        $replace{$word} = "__impossible";
        $lexicon{"__impossible"}{task}{count} += $lexicon{$word}{task}{count};
        $lexicon{"__impossible"}{unadapt}{count} += $lexicon{$word}{unadapt}{count};
        $lexicon{"__impossible"}{seed}{count} += $lexicon{$word}{seed}{count};
	## the count in avail is 0 by construction
        $currmodel{"__impossible"}{count} += $lexicon{$word}{seed}{count};
        delete $lexicon{$word};
    } elsif ( ($mincount > $lexicon{$word}{task}{count})
           && ($mincount > $lexicon{$word}{unadapt}{count}) ){
        ## replace sketchily-supported words (count < 3) in the task
        ## and unadapted distributions.  "Three shall be the number
        ## thou shalt count, and the number of the counting shall be
        ## three." <-- Monty Python
        $replace{$word} = "__dubious";
        $lexicon{"__dubious"}{task}{count} += $lexicon{$word}{task}{count};
        $lexicon{"__dubious"}{unadapt}{count} += $lexicon{$word}{unadapt}{count};
        $lexicon{"__dubious"}{seed}{count} += $lexicon{$word}{seed}{count};
        $lexicon{"__dubious"}{avail}{count} += $lexicon{$word}{avail}{count};
        $currmodel{"__dubious"}{count} += $lexicon{$word}{seed}{count};
        delete $lexicon{$word};
    } elsif ( ( exp(1)  > $lexicon{$word}{ratio} )
	      && ( exp(-1) < $lexicon{$word}{ratio} ) ){
        unless ($keep_boring) {
            ## if keep_boring=1, then 
	    ## mark words with ratio between 0.5 and 2.  we can reasonably
	    ## expect to estimate these probabilities accurately. bucket
	    ## also by their frequency in the unadapted distribution. (the
	    ## reason is because the unadapted distribution will tell us
	    ## what percentage of the available sentences we can expect to
	    ## have associated with the word.)

	    ## the number of zeros after the decimal
	    (my $band = $lexicon{$word}{unadapt}{prob}) =~ s/.*\.(0*).*/$1/;
	    ## the label with which we're replacing the word.
            my $bucket = "__boring__".$band; 
            $replace{$word} = $bucket;
            $lexicon{$bucket}{ratio} = $band;
            $lexicon{$bucket}{task}{count} += $lexicon{$word}{task}{count};
            $lexicon{$bucket}{unadapt}{count} += $lexicon{$word}{unadapt}{count};
            $lexicon{$bucket}{seed}{count} += $lexicon{$word}{seed}{count};
            $lexicon{$bucket}{avail}{count} += $lexicon{$word}{avail}{count};
	    ## initialize the current model with the seed counts
	    $currmodel{$bucket}{count} += $lexicon{$word}{seed}{count};
	    delete $lexicon{$word};
        } elsif ( $lexicon{$word}{ratio} < 1 ){
            ## bucket bad (available-skewed) words by the first digit of
            ## the log of their ratio, which is the same as the power of
            ## e. bucket -3 means the ratio is under e^{-3} but above
            ## e^{-4}. "int" fucntion rounds -3.5 to -3, and 3.5 to 3.
            my $truncate = int( log( $lexicon{$word}{ratio} )); 
            my $bucket = "__".$truncate; ## the label with which we're replacing the word.
            $replace{$word} = $bucket;
            $lexicon{$bucket}{ratio} = $truncate;
            $lexicon{$bucket}{task}{count} += $lexicon{$word}{task}{count};
            $lexicon{$bucket}{unadapt}{count} += $lexicon{$word}{unadapt}{count};
            $lexicon{$bucket}{seed}{count} += $lexicon{$word}{seed}{count};
            $lexicon{$bucket}{avail}{count} += $lexicon{$word}{avail}{count};
            $currmodel{$bucket}{count} += $lexicon{$word}{seed}{count};
            delete $lexicon{$word};
        } else {
            $currmodel{$word}{count} += $lexicon{$word}{seed}{count};
            $replace{$word} = $word;
        }
    }
}
print STDERR "...done\n";

## more efficient to just make another pass through the lexicon than
## to update the currmodel{$word}{prob} a bunch of times.
## "each %hash" is a true iterator, uses less memory than "keys %hash".
while (my ($word, $value) = each %lexicon) {
    ## hconstant is POSITIVE, and the log term in WGE and SGE is always
    ## negative. The gain from adding a 'good' word is a _decrease_ in
    ## perplexity|entropy.
    $lexicon{$word}{hconstant} = $lexicon{$word}{task}{count}/$task_tokens;
    $currmodel{$word}{count} = 0 unless ($currmodel{$word}{count});
    if ($seed_tokens > 0) {
    	$currmodel{$word}{prob}  = $currmodel{$word}{count}/$seed_tokens;
    } else {
    	$currmodel{$word}{prob}  = 0;
    };
}
## construct the squish-vocab corpora, save to disk.
print STDERR " * producing corpus projections with reduced vocab...\n  task = $task_file_squish , and\n  available data = $available_file_squish\n";
unless ($exists_task_file_squish) {
    open (TASK, "<$task") or die "no such file $task: $!";
    binmode TASK, ':encoding(UTF-8)';
    open (TASK_SQUISH, ">$task_file_squish") or die "$!";
    binmode TASK_SQUISH, ':encoding(UTF-8)';
    while(<TASK>){
        chomp;
        foreach (split(' ', $_)) {
            print TASK_SQUISH "$replace{$_} " if ($_);
        }
        print TASK_SQUISH "\n";
    } # while
    close TASK; close TASK_SQUISH;
} # unless
my %available_hash; ## tracks the contents of the available corpus
unless ($exists_available_file_squish) {
    open (AVAILABLE, "<$available") or die "no such file $available: $!";
    binmode AVAILABLE, ':encoding(UTF-8)';
    open (AVAILABLE_SQUISH, ">$available_file_squish") or die "$!";
    binmode AVAILABLE_SQUISH, ':encoding(UTF-8)';
    while(<AVAILABLE>){
    	chomp;
    	## don't store the raw lines in memory; it's too big and we
    	## perform no operations with it. just use the line_id
    	## afterwards to pull out the original line.
    	foreach (split(' ', $_)) {
            print AVAILABLE_SQUISH "$replace{$_} " if ($_);
        }
        print AVAILABLE_SQUISH "\n";
    } # while
    close AVAILABLE; close AVAILABLE_SQUISH;
} # unless
print STDERR "...done\n";

## go through squished available file and set up data structures.
my $smoothing_count = 0.01; ## for when log(0) fails
my %length_penalty;
my $length_cap=250;
my $currmodel_linecount = 0; ## haven't picked any NEW lines yet
my $currmodel_wordcount = $seed_tokens; ## thus also haven't added any words yet
print STDERR " * computing standard penalties for sentence lengths (up to $length_cap tokens)...   \n";
print STDERR "    (recall sentence score = penalty + gain) ";
## first pass through is a little weird because of zero counts
foreach (0..$length_cap) {
    ## first time through, wordcount is 0, and can't take log of 0.
    ## smoothing_count is defined elsewhere and is assumed to be small,
    ## probably 0.01. the factor of 2 is to ensure that the ratio is always
    ## >1 in the $_=0 edge case.
    $length_penalty{$_} = log( ($currmodel_wordcount + $_ + 2*$smoothing_count) 
                                / ($currmodel_wordcount + $smoothing_count) );
}
#&update_length_penalties;
print STDERR "...done\n";

## we don't need to go through the task file again. we have previously modified
## the contents of %lexicon to only cover the squished representation, computed
## $lexicon{$word}{hconstant} and initialized the model score (prob 0, count 0).
print STDERR " * indexing sentences by their vocabulary words...   ";
open (AVAILABLE_SQUISH, "<$available_file_squish") or die "$!";
binmode AVAILABLE_SQUISH, ':encoding(UTF-8)';
while(<AVAILABLE_SQUISH>){
    chomp;
    my @tokens = split(' ', $_);
    ## skip any line that's too long to be good. 
    next if (scalar(@tokens) > $length_cap);
    my $line_id = $.;
    $available_hash{$line_id}{string} = "$_";
    $available_hash{$line_id}{tokencount} = scalar @tokens;
    ## the gain term from adding this line next. total model
    ## improvement score (net benefit if negative, net harmful if
    ## positive) of a sentence is the penalty plus the gain.
    my $sentence_gain_estimate = &compute_sentence_gain_estimate($line_id);
    ## this next loop has some duplicate effort, but we need to link lines
    ## to their words and we only do this process once.
    my %count;
    foreach (@tokens) {
        ## count occurrences of each word per line
        $count{$_}++;
    } # foreach @token
    while (my ($word, $value) = each %count) {
        ## now link each line plus its estimated gain to each word
        ## i.e. associate this line with every word it contains
        my $score = $length_penalty{scalar(@tokens)} + $sentence_gain_estimate;
        my @tmp=($score, $sentence_gain_estimate, $line_id);
        ## have to push a reference to the tuple so that it doesn't
        ## get flattened into one long array!
        push @{$lexicon{$word}{line_list}}, \@tmp;
    } # while each %count
}
close AVAILABLE_SQUISH;
print STDERR "...done\n";

print STDERR " * sorting the indexed sentences...   ";
foreach (keys %lexicon) {
    ## sort the list of lines per word, in place
    if ($lexicon{$_}{line_list}) {
    	my @sorted = sort { $a->[0] <=> $b->[0] } @{$lexicon{$_}{line_list}};
    	$lexicon{$_}{line_list} = \@sorted;
    	print STDOUT "$_\t" . scalar(@sorted) ."\n";
    } else {
    	print STDERR "    deleted, no lines:\t$_\n";
        delete $lexicon{$_};
    }
}
print STDERR "...done\n";

my @word_gain_estimates;
## WGE = "word gain estimate", or "potential entropy improvement from adding a line containing this word to the selected pile"
## maintain 2d array of ($WGE, $word, $hconstant), sorted by $WGE
print STDERR " * building sorted list of words and their gain estimates...   ";
## "each %hash" is a true iterator, uses less memory than "keys %hash".
while (my ($word, $value) = each %lexicon) {
    ## update the word gain estimates.
    $lexicon{$word}{WGE} = $lexicon{$word}{hconstant} * log(
        ($currmodel{$word}{count} + $smoothing_count) /
        ($currmodel{$word}{count} + 1) ); ## something in this calculation is off when words contain underscores
    next if $word eq "__dubious"; ## don't want to select based on sketchy words
    next if $word eq "__useless"; ## don't want to select based on words not in $task
    next if $word eq "__impossible"; ## no point in selecting based on words not in $unadapted
    next if $word =~ m/^__\-[0-9]/; ## no point in selecting based on words biased towards $poo
    next if $word =~ m/^__boring__0{1,7}/; ## don't select on common, unbiased words
    ## only allow it if it's less frequent than 1 in ten million tokens.
    my @tmp = ($lexicon{$word}{WGE}, $word, $lexicon{$word}{hconstant});
    push @word_gain_estimates, \@tmp;
} # while each %lexicon
## keep this list sorted by the word's gain estimate ($WGE): most negative WGE first.
@word_gain_estimates = sort { $a->[0] <=> $b->[0] } @word_gain_estimates;
print STDERR "...done\n";


## ok, now everything is set up and we're ready to start iterating!
## (until we run out of sentences to add)
my $debuggingcounter = keys %available_hash;
my $linecounter = 1;
print STDERR "running max $debuggingcounter iterations!";
while ($debuggingcounter > 0){
    ## get best word from our sorted list
    last unless (@word_gain_estimates);
    my ($WGE,$bestword,$hconstant) = @{$word_gain_estimates[0]};
    my $loop_start_time = gettimeofday( );
    print STDERR "    ===> $debuggingcounter best word\t$bestword\t";
    ## each element of $lexicon{$word}{line_list} is a triple: $score,
    ## $sentence_gain_estimate, $line_id. we just need the line_id.
    my $num_lines_for_bestword = scalar @{$lexicon{$bestword}{line_list}};
    ## sanity check: do we have any lines left?
    if ($num_lines_for_bestword == 0) {
        ## if there are no sentences left, then delete the word.
        print STDERR "no lines left for word $bestword : deleting it from lexicon. ";
        delete $lexicon{$bestword};
        print STDERR scalar (keys %lexicon) . " words left.\n";
    	my (@wordindex) = grep {$word_gain_estimates[$_]->[1] eq $bestword} 0..$#word_gain_estimates;
    	splice @word_gain_estimates, $wordindex[0], 1;
        next; ## pick a new word; no need to update the word estimates themselves
    }

    ## find the first (best) still-active line for that word
    my $first_line_id_for_bestword = 0;
    my @indices_to_prune = ();
    ## all lines with scores < threshold should be updated, all lines
    ## with scores > threshold don't have to be updated.
    my $score_threshold = 0;
    for my $array_index (0..$num_lines_for_bestword-1) {
    	$first_line_id_for_bestword = $lexicon{$bestword}{line_list}[$array_index]->[2];
        ## first check whether the current line is still available.
        if ($available_hash{$first_line_id_for_bestword}) {
    	    ## update the SGE for the best sentence for that best word.
    	    my $sentence_gain_estimate = &compute_sentence_gain_estimate($first_line_id_for_bestword);
    	    print STDERR "updating SGE $lexicon{$bestword}{line_list}[$array_index]->[1] ->$sentence_gain_estimate  ";
    	    ## this is the first/best line, so its score sets the threshold
    	    $score_threshold =
            $length_penalty{$available_hash{$first_line_id_for_bestword}{tokencount}}
    				+ $sentence_gain_estimate;
    	    print STDERR " and sentence score $lexicon{$bestword}{line_list}[$array_index]->[0] -> $score_threshold    ";
                ## actually... don't do the actual update here! it messes up the indices.
    	    last;
    	} else {
    	    ## if line doesn't exist, then it's already been used.
    	    ## splicing it out here will mess up the indices, so just note the index.
    	    push @indices_to_prune, $array_index; # increasing order
    	    next;
    	} # if
    } #for $array_index

    while (@indices_to_prune){
    	my $prune = pop @indices_to_prune; # decreasing order!
    	## now it's safe to delete the already-used entries
    	print STDERR "           pruned the ghost of line "
    	    . $lexicon{$bestword}{line_list}[$prune]->[2] ." (index $prune)\n";
    	splice @{$lexicon{$bestword}{line_list}}, $prune, 1;
    } # foreach @indices_to_prune

    ## recompute the number of sentences left for the best word:
    $num_lines_for_bestword = scalar @{$lexicon{$bestword}{line_list}};
    ## sanity check: do we have any lines left?
    if ($num_lines_for_bestword == 0) {
        ## if there are no sentences left, then delete the word.
        print STDERR "no lines left for word $bestword : deleting it from lexicon. ";
        delete $lexicon{$bestword};
        print STDERR scalar (keys %lexicon) . " words left.\n";
        my (@wordindex) = grep {$word_gain_estimates[$_]->[1] eq $bestword} 0..$#word_gain_estimates;
        splice @word_gain_estimates, $wordindex[0], 1;
        next; ## pick a new word; no need to update the word estimates themselves
    }

    ## now find the location G in @{$lexicon{$bestword}{line_list}} where the
    ## first line _would_ be resorted to _after_ updating its score.
    my @line_list_scores = map { $_->[0] }  @{$lexicon{$bestword}{line_list}};
    ## this next line requires a perl module that isn't common.
    ## do we really need a fancy binary search implementation?
    my $insert_index = binsearch_pos {$a <=> $b} $score_threshold, @line_list_scores;
    ## don't go off the end
    $insert_index = $#line_list_scores if ($insert_index > $#line_list_scores);
    print STDERR "  insert index: $insert_index ";

    my $max_update = $insert_index; ## we'll update all sentences before this one.
    if ($batchmode) {
        ## we want to update at least as many sentences as we're going to
        ## select. here's a big, blunt, instrument: update the first 2*log(k)
        ## lines, where k is # of sentences containing bestword.
        my $sqrt_lines = POSIX::ceil( sqrt(scalar @line_list_scores));
        $max_update = 2*$sqrt_lines if (2*$sqrt_lines > $insert_index);
    }

    ## now update the sentences
    my $update_start_time = gettimeofday( );
    for (0..$max_update) {
        ## iterate backwards through array so pruning ghost lines won't
        ## mess up the indices of the lines yet to update!
        my $i = $max_update - $_;
        my $line_id = $lexicon{$bestword}{line_list}[$i]->[2];
        ## check whether the line has already been selected.
        unless ($available_hash{$line_id}) {
            ## remove the unavailable line
            splice @{$lexicon{$bestword}{line_list}}, $i, 1 ;
#            print STDERR " pruned the ghost of line $line_id\t(index $i).\n";
            next; ## move on to next index
        } # unless
        ## line still available! update the score for this sentence for that best word.
        my $sentence_gain_estimate = &compute_sentence_gain_estimate($line_id);
        my $score = $length_penalty{$available_hash{$line_id}{tokencount}} +
            $sentence_gain_estimate;
        my @tmp=($score, $sentence_gain_estimate, $line_id);
        ## update the entry for that line for this word.
        $lexicon{$bestword}{line_list}[$i] = \@tmp;
    } # for

    ## sanity check: do we have any lines left?
    $num_lines_for_bestword = scalar @{$lexicon{$bestword}{line_list}};
    if ($num_lines_for_bestword == 0) {
        ## if there are no sentences left, then delete the word.
        print STDERR "no lines left for word $bestword : deleting it from lexicon. ";
        delete $lexicon{$bestword};
        print STDERR scalar (keys %lexicon) . " words left.\n";
        my (@wordindex) = grep {$word_gain_estimates[$_]->[1] eq $bestword} 0..$#word_gain_estimates;
        splice @word_gain_estimates, $wordindex[0], 1;
        next; ## pick a new word; no need to re-update the word estimates themselves
    }

    ## ok, so this bestword has some lines, and we've just updated some of the
    ## top lines' scores. resort them.
    my @sorted = sort { $a->[0] <=> $b->[0] } @{$lexicon{$bestword}{line_list}};
    $lexicon{$bestword}{line_list} = \@sorted;
    ## at this point, the best sentence is one whose score we did just update,
    ## either the bestline or something that was estimated to be better than
    ## it, so it is trustworthy.
    my $sort_end_time = gettimeofday( );

    my @goodlines_tuples = ();
    if ($batchmode) {
        ## select the top sqrt(lines) best sentences, and remove them entirely.
    	## we have pruned lines since picking the best sentence. As the array
    	## has (potentially) shrunk, compute the new sqrt(k)
    	my $new_sqrt_lines = POSIX::ceil( sqrt(scalar @sorted));
    	## ah, why the hell not:
    	@goodlines_tuples = splice @{$lexicon{$bestword}{line_list}}, 0, $new_sqrt_lines;
    	## don't add adjacent string-identical duplicates in the same
    	## batch. it probably doesn't make a lick of difference, but
    	## seeing repeated sentences looks bad to humans. *rolleyes*
    	my $prev_string="";
        my @indices_to_unpick = ();
    	for my $i (0..$#goodlines_tuples){
    	    ## actually, the tuples don't contain the string, so this is a right pain.
    	    my $id = $goodlines_tuples[$i]->[2];
    	    my $string = $available_hash{$id}{string};
    	    if ($string eq $prev_string){
                ## mark string-identical line for returning to the pool. the
                ## indices should wind up in ascending order.
                push @indices_to_unpick, ($i);
    	    } else {
        		$prev_string = $string;
    	    }
    	}
        while (@indices_to_unpick) {
            my $array_index = pop @indices_to_unpick; # go in descending index order
            ## put the duplicate back at the front of the un-selected list
            unshift @{$lexicon{$bestword}{line_list}}, (splice @goodlines_tuples, $array_index, 1);
            ## this decreases the size of the batch!
        } # while
    	print STDERR "adding ".scalar(@goodlines_tuples)." of $num_lines_for_bestword lines to corpus  ";
    } else {
        ## no batchmode, just take first sentence.
    	@goodlines_tuples = splice @{$lexicon{$bestword}{line_list}}, 0, 1;
    	print STDERR "adding ".scalar(@goodlines_tuples)." of $num_lines_for_bestword lines to corpus  ";
    } # if-else $batchmode

    ## note we're not removing this line from the line_lists for the other words it contains.
    ## each goodline_tuple is ($score, $sentence_gain_estimate, $line_id);
    while (@goodlines_tuples) {
    	$currmodel_linecount++; ## give this line its new rank
    	my $tuple = shift @goodlines_tuples; ## first one is best-scoring one
    	my ($goodline_score, $goodline_sge, $goodline_id) = @{$tuple};
    	## as usual, first check whether the line has previously been selected.
        unless ($available_hash{$goodline_id}) {
            ## this line was already added, but we'd just spliced it
            ## out of @{$lexicon{$bestword}{line_list}} anyway!
            print STDERR "           ignored the ghost of goodline $goodline_id.\n";
            next; ## move on
        } # unless
    	my @goodline_tokens = split(' ', $available_hash{$goodline_id}{string});
    	## JADED columns are: sentence {score, penalty, gain, output rank,
    	## input line_id}, the root word, WGE, and the squished line.
    	$currmodel_score += $goodline_score;
    	print JADED join("\t", ( $currmodel_score, $goodline_score,
    				 $length_penalty{@goodline_tokens},
    				 $goodline_sge, $currmodel_linecount,
    				 $goodline_id, $bestword, $WGE,
    				 $available_hash{$goodline_id}{string} ) ) . "\n";
    	delete $available_hash{$goodline_id}; ## remove the line entirely
    	## update the currmodel with the contents of the sentence.
    	my %count;
    	foreach (@goodline_tokens) {
    	    $count{$_}++; ## count occurrences of each word type
    	} # foreach
    	## update the current model wordcount & linecount
    	$currmodel_wordcount += scalar(@goodline_tokens);
    	while (my ($word, $value) = each %count) {
    	    ## update the count for this word
    	    $currmodel{$word}{count} += $count{$word};
    	} # while %count
    } # while @goodlines_tuples
#    print STDERR " :: added lines at ". (gettimeofday()-$loop_start_time)." ::";

    ## sanity check: do we have any lines left?
    $num_lines_for_bestword = scalar @{$lexicon{$bestword}{line_list}};
    if ($num_lines_for_bestword == 0) {
    	## if there are no sentences left, then delete the word.
    	print STDERR "no lines left for word $bestword : deleting it from lexicon. ";
    	delete $lexicon{$bestword};
    	print STDERR scalar (keys %lexicon) . " words left.\n";
    	my (@wordindex) = grep {$word_gain_estimates[$_]->[1] eq $bestword} 0..$#word_gain_estimates;
        splice @word_gain_estimates, $wordindex[0], 1;	next; ## pick a new word; no need to update the word estimates themselves
    }

    ## update the lexicon hash with what we just learned.
    @word_gain_estimates=(); ## clear the decks
    while (my ($word, $value) = each %lexicon) {
    	## update the word gain estimatte
    	$lexicon{$word}{WGE} = $lexicon{$word}{hconstant}
        * log( ($currmodel{$word}{count} + $smoothing_count) / ($currmodel{$word}{count} + 1) );
    	## compute each word's new empirical probability
        $currmodel{$word}{prob} = $currmodel{$word}{count} / $currmodel_wordcount;
        next if $word eq "__dubious"; ## don't want to select based on sketchy words
        next if $word eq "__useless"; ## don't want to select based on words not in $task
        next if $word eq "__impossible"; ## no point in selecting based on words not in $available
        next if $word =~ m/^__\-[0-9]/; ## no point in selecting based on words biased towards $unadapted
#        next if ($keep_boring < 1) && ($word =~ m/^__boring__0{1,7}/); ## don't select on common, unbiased words
        ## if $keep_boring==1, then there are no __boring tokens and the next line won't fire.
        next if $word =~ m/^__boring__0{1,7}/; ## don't select on common, unbiased words. only allow it if it's less frequent than 1 in ten million tokens.
        my @tmp = ($lexicon{$word}{WGE}, $word, $lexicon{$word}{hconstant});
        push @word_gain_estimates, \@tmp;
    } # foreach

    ## resort the words.
    ## keep this list sorted by the word's gain estimate ($WGE): most negative WGE first.
    if (scalar(@word_gain_estimates) == 0) { print STDERR "Out of words!\n"; last;};
    @word_gain_estimates = sort { $a->[0] <=> $b->[0] } @word_gain_estimates;

    ## update the length penalties for the next sentence we add
    &update_length_penalties();
    $debuggingcounter--;
    $linecounter++;
    my $elapsed = gettimeofday() - $loop_start_time;
    print STDERR " :: finished in $elapsed sec total.\n";
}

print STDOUT "=======================";

sub update_length_penalties {
    ## compute the penalties for adding a certain number of words to the
    ## selected set. The base case (where currmodel_wordcount=0) is handled above.
    foreach (0..$length_cap) {
    	$length_penalty{$_} = log( ($currmodel_wordcount + $_)/($currmodel_wordcount) );
    }
}

sub compute_sentence_gain_estimate {
    my ($line_id, @bad) = @_;
    die "Extra args to compute_sentence_gain_estimate \n" if @bad;
    ## the updated gain term from adding this line next.
    my @tokens = split(' ', $available_hash{$line_id}{string});
    my $sentence_gain_estimate = 0;
    my %count;
    foreach (@tokens) {
    	$count{$_}++; ## count occurrences of each word type
    }
    while (my ($word, $value) = each %count) {
    	## compute the estimated gain for the sentence
    	$sentence_gain_estimate += ($lexicon{$word}{hconstant}
            * log( ($currmodel{$word}{count} + $smoothing_count) 
                    / ($currmodel{$word}{count} + $count{$word}) ) );
    } # while
    ## update the sentence's estimated gain
    $available_hash{$line_id}{SGE} = $sentence_gain_estimate;
    return $sentence_gain_estimate;
} # sub

exit;

## The MIT License (MIT)
##
## Copyright 2012-2017 amittai axelrod
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
