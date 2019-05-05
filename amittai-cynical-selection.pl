#!/usr/bin/perl
use warnings;
use strict;
use diagnostics;
use Getopt::Long;
use Data::Dumper; ## for debugging
$Data::Dumper::Indent = 1; ## for debugging
$Data::Dumper::Sortkeys = 1; ## for debugging
$Data::Dumper::Useqq = 1; ## for debugging

print STDERR "Begin at ".(localtime)."\n";

use open ':std', ':encoding(UTF-8)'; ## use utf8 for stdin/stdout/stderr
use FileHandle;
use POSIX;
use sort '_mergesort'; ## take advantage of mostly-ordered arrays
no sort 'stable'; ## don't care if stable; speed matters more
## you may need to install the first modules.
## $  sudo cpan install List::MoreUtils
## $  sudo cpan install Time::HiRes
## $  sudo cpan install Sort::Key::Top
## $  sudo cpan install Test::Simple
## $  sudo cpan install Array::Heap
## $  sudo cpan install Array::Heap::ModifiablePriorityQueue
## $  sudo cpan install autovivification  # yes, lowercase
use List::Util qw( reduce max );  ## reduce finds 1-best value in hash
use List::MoreUtils qw( uniq lastidx );
use Sort::Key::Top; ## get top n values.
use Array::Heap::ModifiablePriorityQueue;
## fantastic: https://metacpan.org/pod/Array::Heap::ModifiablePriorityQueue
use Time::HiRes;  ## to get timing info for debugging

## original copyright amittai axelrod 2012. Released under MIT License
## (included at end).  if you port or update this code, i'd appreciate
## a copy.

STDERR->autoflush(1); ## make STDERR 'hot' (no buffering); good for debugging
STDOUT->autoflush(1); ## make STDOUT 'hot' (no buffering); good for debugging

sub compute_sentence_gain_estimate;
sub update_length_penalties;
sub find_best_word_gain_estimate;
sub count_num_lines_for_word;

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

my ($task_vocab, $unadapt_vocab, $available, $seed_vocab, $stats,
    $working_dir, $jaded, $mincount, $maxcount, $keep_boring,
    $batchmode, $numlines, $save_memory)
      = ('','','', '','','', '','','', '','','', '');

GetOptions ("task_vocab=s" => \$task_vocab,
    "unadapt_vocab=s"          => \$unadapt_vocab,
    "available=s"              => \$available,
    "seed_vocab=s"             => \$seed_vocab,
    "stats=s"                  => \$stats,
    "working_dir=s"            => \$working_dir,
    "jaded=s"                  => \$jaded,
    "mincount=s"               => \$mincount,
    "maxcount=s"               => \$maxcount,
    "keep_boring"              => \$keep_boring,
    "batchmode"                => \$batchmode,
    "numlines"                 => \$numlines,
    "save_memory"              => \$save_memory);

## set default values unless explicitly passed by user
$working_dir ||= ".";
$mincount    ||= 3;
$batchmode   ||= 0;
$keep_boring ||= 1;
$numlines    ||= 0;   ## max number of lines to pick. 0 means 'all'.
$save_memory ||= 0;   ## use maxcount to reduce memory usage. runs slower.
(my $available_file = $available) =~ s/^.*\/(.*?)$/$1/; ## strip path
my $exists_seed_vocab = (-e $seed_vocab) || "";

print STDERR "flags: dir $working_dir , mincount $mincount "
  .", batchmode $batchmode , keep_boring $keep_boring, save_memory $save_memory\n";

open (JADED, ">$jaded") or die "$!";
binmode JADED, ':encoding(UTF-8)';
JADED->autoflush(1); ## no buffering?

## a note on the STATS file produced by amittai-vocab-ratios.pl
## The output columns are: word, prob1/prob2, prob1, count1, prob2, count2.
## Usually used with model1=in-domain task and model2= general data, making the
## ratio prob1/prob2 "number of times more likely the word is in the in-domain
## corpus than in the general corpus".
my $task_tokens      = 0;
my $unadapted_tokens = 0;
my $seed_tokens      = 0;

## %selectable_words_hash will contain the only words we can use to select
## lines, and their WGE. WGE = "word gain estimate", or "potential entropy
## improvement from adding a line containing this word to the selected pile".
## %ultracommon_words_hash contains words that ought to be %selectable, but
## %that appear in too many lines and would explode the memory requirements.
## %secondpass_words_hash has keys for everything else in the lexicon.
my %lexicon_hash;
my %selectable_words_hash;
my %ultracommon_words_hash;
my %secondpass_words_hash;
## ideally, %ultracommon is a subset of %selectable, and the union of
## %selectable and %secondpass is the complete %lexicon.
my $selectable_words_queue = Array::Heap::ModifiablePriorityQueue->new();

## initialize the lexicon with stats computed from task & unadapted.
print STDERR " * going through the task+unadapted distribution vocabulary statistics...   ";
open (STATS, "<$stats") or die "no such file $stats: $!";
binmode STATS, ':encoding(UTF-8)';
while(<STATS>){
    chomp;
    next if ($_ =~ m/^$/);
    my ($word,$ratio,$prob1,$count1,$prob2,$count2) = split(' ', $_);
    ## these are based on the task and unadapted distributions
    $lexicon_hash{$word}{ratio}=$ratio;
    $lexicon_hash{$word}{task}{count}=$count1;
    $lexicon_hash{$word}{unadapt}{count}=$count2;
    $lexicon_hash{$word}{task}{prob}=$prob1;
    $lexicon_hash{$word}{unadapt}{prob}=$prob2;
    $task_tokens += $count1;
    $unadapted_tokens += $count2;
    ## these are computed from the "seed" and "available" corpora
    $lexicon_hash{$word}{seed}{count}=0;
    $lexicon_hash{$word}{avail}{count}=0;
    ## initialize WGE to a positive (lousy) value.
    $selectable_words_hash{$word} = 1;
}
close STATS;
print STDERR "...done\n";

## read in vocabulary statistics for the seed file, if one is provided.
if ($exists_seed_vocab) {
    print STDERR " * going through the seed vocabulary...   ";
    open (SEED_VOCAB, "<$seed_vocab") or die "no such file $seed_vocab: $!";
    binmode SEED_VOCAB, ':encoding(UTF-8)';
    while(<SEED_VOCAB>){
    	chomp;
    	next if ($_ =~ m/^$/);
    	my ($word,$prob,$count) = split(' ', $_);
    	$lexicon_hash{$word}{seed}{count} = $count;
    	$seed_tokens += $count;
    	## add default values as needed
        $lexicon_hash{$word}{task}{count}    ||= 0;
        $lexicon_hash{$word}{unadapt}{count} ||= 0;
        $lexicon_hash{$word}{avail}{count}   ||= 0;
        ## initialize WGE to a positive (lousy) value.
        $selectable_words_hash{$word} = 1;
    } # while
    close SEED_VOCAB;
    print STDERR "...done\n";
} # if

## variables related to length penalties
my %length_penalty;
my $smoothing_count = 0.01; ## for when log(0) fails
my $length_cap = 100;
my $currmodel_linecount = 0; ## haven't picked any NEW lines yet
my $currmodel_wordcount = $seed_tokens; ## thus also haven't added any words yet
print STDERR " * computing standard penalties for sentence lengths (up to $length_cap tokens)...   \n";
print STDERR "    (recall sentence score = penalty + gain) ";
## first set of length penalties is a little weird because of zero counts
foreach (0..$length_cap) {
    ## first time through, wordcount is 0, and can't take log of 0.
    ## smoothing_count is defined elsewhere and is assumed to be small,
    ## probably 0.01. the factor of 2 is to ensure that the ratio is always
    ## >1 in the $_=0 edge case.
    $length_penalty{$_} = log( ($currmodel_wordcount + $_ + 2*$smoothing_count)
                             / ($currmodel_wordcount + $smoothing_count) );
}
#update_length_penalties;
print STDERR "...done\n";

print STDERR " * going through the vocabulary of the available lines...   ";
open (AVAILABLE, "<$available") or die "no such file $available: $!";
binmode AVAILABLE, ':encoding(UTF-8)';
my $available_N = 0;
my %available_lines_hash; ## tracks the contents of the available corpus
## garbage lines, save them so the output corpus is the same length as the input.
my %junk_lines_hash;
## lines to use to top up {line_list} for ultracommon words
my %ultracommon_lines_hash;
my @ultracommon_lines_array; # for speed

while(<AVAILABLE>){
    chomp;
    my @tokens = split(' ', $_);
    my $line_id = $.;
    if (($_ =~ m/^$/) or (scalar(@tokens) > $length_cap)) {
        ## skip any line that's empty, or too long.
        $junk_lines_hash{$line_id}{string} = "$_";
        next;
    }
    $available_lines_hash{$line_id}{string} = "$_";
    $available_lines_hash{$line_id}{tokencount} = scalar @tokens;
    $available_N++;
    foreach my $word (@tokens) {
        $lexicon_hash{$word}{avail}{count}++;
        ## add default values as needed
        $lexicon_hash{$word}{task}{count}    ||= 0;
        $lexicon_hash{$word}{seed}{count}    ||= 0;
        $lexicon_hash{$word}{unadapt}{count} ||= 0;
        ## initialize WGE to a positive (lousy) value.
        $selectable_words_hash{$word} = 1;
    }
} # while (<AVAILABLE>)
close AVAILABLE;
print STDERR "...done. ". scalar (keys %junk_lines_hash) ." junk lines found.\n";
## maxcount cutoff: when $save_memory is enabled, only $maxcount lines of
## AVAILABLE will appear in {line_list} for each selectable word. this limits
## the amount of data in memory at once. this is unrelated to batchmode.
$maxcount ||= POSIX::ceil(sqrt($available_N)); ## default is sqrt(N)
my $refill_line = POSIX::ceil($maxcount * 0.2); ## restock when we drop below this level
print STDERR "maxcount = $maxcount (of $available_N) lines";
print STDERR ", refilling at $refill_line" if ($save_memory);
print STDERR ".\n";
## see $batchsize variable below to control how many lines of each $bestword
## we should select at a time. this controls how quickly {line_list} gets
## emptied. $batchsize only applies in batchmode.

## at this point %lexicon_hash should contain all possible words we could know
## about in all {task+unadapted,seed,avail} vocabularies.

my %currmodel_hash; ## the model we update as we go (initialized with SEED)
my $currmodel_score; ## entropy of the task set using our model

print STDERR " * going though the lexicon...   ";
## %secondpass_words_hash{$word} is -1 if the word isn't used because it's
## %unhelpful, and a positive value if the word is otherwise helpful but too
## %common.
## "each %hash" is a true iterator, uses less memory than "keys %hash".
while (my ($word, $throwaway_hashvalue) = each %lexicon_hash) {
    ## seed $currmodel with the contents of SEED
    $currmodel_hash{$word}{count} ||= 0;
    $currmodel_hash{$word}{count} += $lexicon_hash{$word}{seed}{count};
    ## hconstant = precomputable part of task perplexity computation:
    ## [C_task(v)/C(task)]. hconstant is POSITIVE, and the log term in WGE and
    ## SGE is always negative. The gain from adding a 'good' word is a
    ## _decrease_ in perplexity or entropy. We need hconstant for every word
    ## in the lexicon, whether selectable or not.
    $lexicon_hash{$word}{hconstant} = $lexicon_hash{$word}{task}{count}/$task_tokens;
    if ($seed_tokens > 0) {
        $currmodel_hash{$word}{prob}  = $currmodel_hash{$word}{count}/$seed_tokens;
    } else {
        $currmodel_hash{$word}{prob}  = 0;
    };
    ## now decide which words are selectable
    if ($word =~ m/^__/) {
        ## don't mess with any labels we've created
        delete $selectable_words_hash{$word};
        ## ...but we'll use them after all the good stuff is gone.
        $secondpass_words_hash{$word} =  -1;
    } elsif (0 == $lexicon_hash{$word}{task}{count}) {
        ## don't pick useless words (task count = 0, so we don't care about it)
        delete $selectable_words_hash{$word};
        ## ...but we'll use them after all the good stuff is gone
        $secondpass_words_hash{$word} = -1;
    } elsif (0 == $lexicon_hash{$word}{avail}{count}) {
    	## these words will have 0 candidate sentences affiliated with
    	## them. regardless of what's in the task or seed, we can't do
    	## anything with them. remove to avoid having empty arrays.
        delete $selectable_words_hash{$word};
    } elsif ( ($mincount > $lexicon_hash{$word}{task}{count})
           && ($mincount > $lexicon_hash{$word}{unadapt}{count}) ){
        ## don't select based on sketchily-supported words (count < 3 in the
        ## task and unadapted distributions).  "Three shall be the number thou
        ## shalt count, and the number of the counting shall be three." <--
        ## Monty Python
        delete $selectable_words_hash{$word};
        ## ...but we'll use them after all the good stuff is gone
        $secondpass_words_hash{$word} = - 1;
    } elsif ( ( exp(1)  > $lexicon_hash{$word}{ratio} )
	       && ( exp(-1) < $lexicon_hash{$word}{ratio} ) ){
        if ($keep_boring) {
            ## filter only based on count
            if ($lexicon_hash{$word}{avail}{count} > $maxcount) {
                ## too common in AVAIL! will cost too much to update and sort
                ## all of it. these words go into %ultracommon, AND
                ## %selectable. we will be limiting what goes into
                ## {line_list}, if $save_memory is enabled.
                $ultracommon_words_hash{$word}{words_over}
                    = $lexicon_hash{$word}{avail}{count} - $maxcount;
                ## to check whether we actually have more than $maxcount lines
                $ultracommon_words_hash{$word}{lines_over} = -$maxcount;
            }
            ## otherwise, do nothing and treat it as normal.
        } else {
            ## if keep_boring=0, then don't select initially based on these words.
            delete $selectable_words_hash{$word};
            ## ...but we'll use them after all the good stuff is gone
            $secondpass_words_hash{$word} = -1;
    	}
    } elsif ( $lexicon_hash{$word}{ratio} < exp(-1) ){
        ## words skewed more than an order of magnitude away from $task are
        ## not indicative of sentences we want.
        delete $selectable_words_hash{$word};
        ## ...but we'll use them after all the good stuff is gone
        $secondpass_words_hash{$word} = -1;
    } else {
	    ## word ratio looks fine.
        if ($lexicon_hash{$word}{avail}{count} > $maxcount) {
            ## too common in AVAIL! will cost too much to update and sort
            ## all of it. these words go into %ultracommon, AND
            ## %selectable. we will be limiting what goes into
            ## {line_list}, if $save_memory is enabled.
            $ultracommon_words_hash{$word}{words_over}
                = $lexicon_hash{$word}{avail}{count} - $maxcount;
                ## to check whether we actually have more than $maxcount lines
            $ultracommon_words_hash{$word}{lines_over} = -$maxcount;
        }  ## otherwise, do nothing and treat it as normal.
    } # if ($word
} # while each %lexicon_hash)
print STDERR "...done\n";

print STDERR " * indexing sentences by their selectable vocabulary words...   ";
while (my ($line_id, $throwaway_hashvalue) = each %available_lines_hash) {
    ## initial scores are all garbage anyway. initialize to random.
    my $random_score = 10 + rand();
    $available_lines_hash{$line_id}{SGE} = $random_score;
    my @tokens = split(' ', $available_lines_hash{$line_id}{string});
    $available_lines_hash{$line_id}{score} = $random_score;
    ## count occurrences of each word per line
    my %count;
    $count{$_}++ for (@tokens);
    while (my ($word, $throwaway_hashvalue_innerloop) = each %count) {
        ## now link each line plus its estimated gain to each word
        ## i.e. associate this line with every *selectable word*
        ## it contains. the restriction to selectable_words keeps
        ## the hash size manageable (ish).
        next unless defined $selectable_words_hash{$word};
        if (defined $ultracommon_words_hash{$word}){
            ## count how many lines we have more than $maxcount
            $ultracommon_words_hash{$word}{lines_over}++;
            if ($save_memory) {
                ## associate ultracommon words with their lines
                $ultracommon_lines_hash{$line_id}{$word}++;
                next if ($ultracommon_words_hash{$word}{lines_over} > 0);
                ## we're over the cap, and $save_memory is on
            } # if $save_memory
        } # defined $ultracommon_words_hash{$word}
        ## we're under the $maxcount cap, or $save_memory is off
        push @{$lexicon_hash{$word}{line_list}}, [$random_score, $line_id];
    } # while each %count
} # while each %available_lines_hash)
print STDERR "...done\n";

print STDERR " * filter the ultracommon words...\n";
while (my ($word, $throwaway_hashvalue) = each %ultracommon_words_hash) {
    ## because sometimes words appear more than once per line.
    ## we had to know which words were ultracommon *before* going through the
    ## corpus, in order to mark the lines that contained them. we used the
    ## wordcount as a proxy; now we can switch to using the linecount.
    if ($ultracommon_words_hash{$word}{lines_over} <= 0) {
        ## this word actually isn't ultracommon after all.
        delete $ultracommon_words_hash{$word};
    } else {
        ## if space is an issue, empty out {line_list} so we can fill it
        ## properly below.
        @{$lexicon_hash{$word}{line_list}} = () if $save_memory;
    }
    print STDERR "weird word: ' $word '\n" if (! defined $word);
} # while each %ultracommon_words_hash
print STDERR "we've got ". scalar (keys %ultracommon_words_hash) ." ultra words\n";

my %ultracommon_words_hash_tmp = %ultracommon_words_hash;
print STDERR " * sift k-best lines for ultracommon words...";
## make a temporary clone of %ultracommon_words_hash that we can destroy
while (my ($word, $throwaway_hashvalue) = each %ultracommon_words_hash_tmp) {
    ## have we seen it yet?
    $ultracommon_words_hash_tmp{$word}{uncovered} = 1;
    ## how many more lines can we put on its now-empty stack?
    if ($save_memory){
        $ultracommon_words_hash_tmp{$word}{uncapped} = $maxcount;
    } # if $save_memory
} # while each %ultracommon_words_hash_tmp

## go through the %available_lines_hash, and randomly populate {line_list} for
## the ultracommon words.
my @batchlines_init;
foreach my $line_id (
  sort { $available_lines_hash{$a}{score} <=> $available_lines_hash{$b}{score} }
  keys %available_lines_hash ){
    ## get the unique words per line
    my %count;
    $count{$_}++ for split(' ', $available_lines_hash{$line_id}{string});
    foreach my $word (keys %count) {
        next unless defined $ultracommon_words_hash_tmp{$word};
        if ($ultracommon_words_hash_tmp{$word}{uncovered}){
            ## we want one line for each ultracommon to start with.
            ## don't worry about repeats right now.
            push @batchlines_init,
                [$available_lines_hash{$line_id}{score}, $line_id];
            delete $ultracommon_words_hash_tmp{$word}{uncovered};
            next;
        }
        if ($save_memory) {
            ## (implicit: we're under the cap, and we nuked line_list earlier)
            ## push the line to make the new {line_list}.
            push @{$lexicon_hash{$word}{line_list}},
                [$available_lines_hash{$line_id}{score}, $line_id];
            ## decrement number of lines left to pick for this word
            $ultracommon_words_hash_tmp{$word}{uncapped}--;
            ## delete the word from the tmp hash if we've hit the cap
            if ($ultracommon_words_hash_tmp{$word}{uncapped} <= 0) {
                delete $ultracommon_words_hash_tmp{$word};
                # print STDERR "  $word      best line & score: "
                #  . $lexicon_hash{$word}{line_list}->[0][1] ." , "
                #  . $lexicon_hash{$word}{line_list}->[0][0] ."\n";
                # print STDERR "        worst line & score: "
                #  . $lexicon_hash{$word}{line_list}->[-1][1] ." , "
                #  . $lexicon_hash{$word}{line_list}->[-1][0] ."\n";
            } # if ($ultracommon_words_hash_tmp{$word}{uncapped} <= 0)
        } # if ($save_memory)
    } # foreach keys %count
    ## quit sieving if we've populated all the ultracommon words.
    last if (scalar keys %ultracommon_words_hash_tmp == 0);
} # foreach sorted line in %available_lines_hash
undef %ultracommon_words_hash_tmp;
print STDERR "   done.\n";

print STDERR "to here: ". scalar (keys %available_lines_hash) ." lines available\n";
print STDERR "to here: ". scalar (keys %selectable_words_hash) ." selectable words\n";
print STDERR "to here: ". scalar (keys %ultracommon_lines_hash) ." ultra lines\n";
print STDERR "to here: ". scalar (keys %ultracommon_words_hash) ." ultra words\n";

## now process (select) @batchlines_init, to start off having seen every
## ultracommon word once. this makes initial estimates reasonable.
my %batchlines_init_count;
my $linecounter = 0;
while (@batchlines_init) {
    my $init_tuple = shift @batchlines_init;
    my ($initline_score, $initline_id) = @{$init_tuple};
    next unless defined $available_lines_hash{$initline_id}; ## already added
    $currmodel_linecount++; ## give this line its new rank
    my @initline_tokens
      = split(' ', $available_lines_hash{$initline_id}{string});
    my %initline_words;
    foreach (@initline_tokens) {
        $initline_words{$_}++;
        $batchlines_init_count{$_}++; ## count occurrences of each word type
    } # foreach (@initline_tokens)
    foreach (keys %initline_words){
        ## keep track of ultracommon_word usage
        $ultracommon_words_hash{$_}{lines_over}--
          if defined $ultracommon_words_hash{$_};
        ## update the count for this word
        $currmodel_hash{$_}{count} += $initline_words{$_};
        ## decrement the still un-selected count for this word
        $lexicon_hash{$_}{avail}{count} -= $initline_words{$_};
    }
    my $initline_sge = $available_lines_hash{$initline_id}{SGE};
    # $currmodel_score += $initline_score;
    ## JADED columns: sentence {input line_id, output rank, score,
    ## penalty, gain}, total score, the root word, WGE, and the squished line.
    print JADED join("\t", ( $initline_id, $currmodel_linecount,
        "-9999", $length_penalty{scalar @initline_tokens},
        $initline_sge, 0, "INIT", "-9999",
        $available_lines_hash{$initline_id}{string} ) ) . "\n";

    ## update the currmodel wordcount
    $currmodel_wordcount += $available_lines_hash{$initline_id}{tokencount};

    delete $available_lines_hash{$initline_id}; ## remove the line entirely
    delete $ultracommon_lines_hash{$initline_id}
      if defined $ultracommon_lines_hash{$initline_id};
    $linecounter++;
    last if $linecounter == $numlines; ## ok to drop everything after this line.
} # while @batchlines_init
print STDERR "    initialized JADED with $currmodel_linecount lines.\n";
@ultracommon_lines_array = keys %ultracommon_lines_hash if $save_memory;

print STDERR " * compute gain estimates for all selectable words...  ";
## ...and build the priority queue for selectable_words
while (my ($word, $throwaway_hashvalue) = each %selectable_words_hash) {
    ## update the word gain estimates for all selectable words.
    ## the value is the WGE itself.
    $selectable_words_hash{$word} = $lexicon_hash{$word}{hconstant} *
      log( ($currmodel_hash{$word}{count} + $smoothing_count) /
           ($currmodel_hash{$word}{count} + 1)   );
    ## copying it to %lexicon_hash just in case.
    $lexicon_hash{$word}{WGE} = $selectable_words_hash{$word};
    ## go building the priority queue (heap) for selectable words
    $selectable_words_queue->add($word, $lexicon_hash{$word}{WGE});
} # while each %selectable_words_hash
print STDERR "...done\n";

print STDERR " * sorting lines for selectable words...  ";
while (my ($word, $throwaway_hashvalue) = each %selectable_words_hash) {
    # ## sort the lines for each word, in place, lowest score first.
    # @{$lexicon_hash{$word}{line_list}}
    #     = sort { $a->[0] <=> $b->[0] } @{$lexicon_hash{$word}{line_list}};
    # the lines were pushed in sorted order already.
    print STDOUT "$word\t" . scalar @{$lexicon_hash{$word}{line_list}} ."\n";
}
print STDERR "...done\n";

## ok, now everything is set up and we're ready to start iterating!
## (until we run out of sentences to add)
my $iterations = scalar keys %available_lines_hash;
my $loopcounter = 1;
print STDERR "running max $iterations iterations!\n";
my $t0 = [Time::HiRes::gettimeofday];
while ($iterations > 0){
    $t0 = [Time::HiRes::gettimeofday];
    print STDERR "===> $iterations ";

    ## find word with lowest WGE, in O(v) time.
    my $bestword = find_best_word_gain_estimate(\%selectable_words_hash, \$selectable_words_queue);
    last if (! defined $bestword);  ## undef means no words left

    # print STDERR "       :: checkpoint 02 " . Time::HiRes::tv_interval($t0)
    #   . " // " . Time::HiRes::tv_interval($t0) ."\n";
    #   my $t02 = [Time::HiRes::gettimeofday];

    ## prune all unavailable lines (keep lines that are still in %available_lines)
    my @tmp_list;
    if ($save_memory and defined $ultracommon_words_hash{$bestword}){
        ## keep {line_list} small for performance
        @tmp_list  = List::MoreUtils::uniq(
          grep { defined $available_lines_hash{@{$_}[1]} }
              @{$lexicon_hash{$bestword}{line_list}} );
        $lexicon_hash{$bestword}{line_list} = \@tmp_list;
    }# ($save_memory and defined $ultracommon_words_hash{$bestword})
    else {
        if (rand() < 0.01) {
            ## once in a while, prune the entire line_list.
            $lexicon_hash{$bestword}{line_list}
              = [ grep { defined $available_lines_hash{@{$_}[1]} }
                @{$lexicon_hash{$bestword}{line_list}} ];
        }
    }

    ## count the lines remaining for that word (and remove word if none left).
    my $num_lines_for_bestword = count_num_lines_for_word(\$bestword);
    ## skip word if no lines left
    next if ($num_lines_for_bestword == 0);

    print STDERR "      best word    $bestword    ";
    print STDERR "word stats: selectable "
      . sprintf("%.6g", $selectable_words_hash{$bestword})
      .", total count $lexicon_hash{$bestword}{avail}{count}, ";
    print STDERR " [ultracommon]" if defined $ultracommon_words_hash{$bestword};

    ## here's where normal and batchmode differ.
    my $batchsize = 1;  ## default, no batchmode
    if ($batchmode){
        if ($save_memory and defined $ultracommon_words_hash{$bestword}){
            ## don't do smaller batches for ultracommons
            $batchsize = POSIX::ceil( sqrt($maxcount) );
        } else {
            $batchsize = POSIX::ceil( sqrt($num_lines_for_bestword) );
        }
        ## we can guarantee $batchsize =< $num_lines_for_bestword
    } # if (($batchmode)

    print STDERR "\n    current batchsize = $batchsize // $num_lines_for_bestword\n";
    print STDERR "     :: 1best word done in " . Time::HiRes::tv_interval($t0)
      . " // " . Time::HiRes::tv_interval($t0) ."\n";
    my $t2 = [Time::HiRes::gettimeofday];

    ## update the sentence scores of the first 2x$batchsize lines.
    my $worst_batch_score = -100;
    my $updatesize        = 2*$batchsize;
    $updatesize           = $num_lines_for_bestword
      if ($updatesize > $num_lines_for_bestword);
    $updatesize           = 0 if ($updatesize < 0);
    my $update_index      = 0;
    my @indices_to_prune  = ();
    my %just_updated_lines;
    while ($update_index < $updatesize){
        my $tmp_line  = $lexicon_hash{$bestword}{line_list}[$update_index]->[1];
        my $tmp_sge   = compute_sentence_gain_estimate(\$tmp_line);
        if (! defined $tmp_sge){
            ## this line is a ghost
            push @indices_to_prune, $update_index; # increasing order
            $update_index++;
            ## don't count it, but don't go off the end of the array
            $updatesize++ if ($updatesize < $num_lines_for_bestword);
            next;
        }
        my $tmp_score = $length_penalty{$available_lines_hash{$tmp_line}{tokencount}}
           + $tmp_sge;
        ## share score information across stacks! use when sifting.
        ## the SGE is already updated in &compute_sentence_gain_estimate
        $available_lines_hash{$tmp_line}{score} = $tmp_score;
        $lexicon_hash{$bestword}{line_list}[$update_index]->[0] = $tmp_score;
        $just_updated_lines{$tmp_line}++;
        ## track the lousiest (most positive) of the batch scores. this is the
        ## update threshold for the rest of the array.
        $worst_batch_score = $tmp_score if ($tmp_score > $worst_batch_score);
        $update_index++;
        if ($save_memory){
            ## did we just update a line containing a different ultracommon? it's a
            ## decent line; push it onto the ultracommon's stack. we want to
            ## refill ultras' stacks less often, because those are 1000x as
            ## expensive as a regular update.
            my @tokens = split(' ', $available_lines_hash{$tmp_line}{string});
            my %count;
            $count{$_}++ for (@tokens);
            while (my ($word, $throwaway_hashvalue_innerloop) = each %count) {
                ## only ultracommon stacks are incomplete
                next unless defined $ultracommon_words_hash{$word};
                next if ($word eq $bestword); ## duh. don't add to its own stack.
                ## skip if we're wayyy overloaded already, to save memory.
                next if ((defined $lexicon_hash{$word}{line_list})
                 && (scalar @{$lexicon_hash{$word}{line_list}} > 5*$maxcount));
                ## add the just-updated line to the stack.
                push @{$lexicon_hash{$word}{line_list}}, [$tmp_score, $tmp_line];
            } # while each %count
        } # if $save_memory
    } # foreach (0..$updatesize-1)
    print STDERR "    updated sentence scores range from ("
        . sprintf("%.6g", $lexicon_hash{$bestword}{line_list}[0]->[0]) . ", #"
        . $lexicon_hash{$bestword}{line_list}[0]->[1]
        . ") to (". sprintf("%.6g", $lexicon_hash{$bestword}{line_list}[$updatesize-1]->[0]) .", #"
        . $lexicon_hash{$bestword}{line_list}[$updatesize-1]->[1] . ") \n";

# move to schwartzian transform?
    ## refresh the scores for the rest of the lines, but don't recompute.
    foreach ( $updatesize..$#{$lexicon_hash{$bestword}{line_list}} ){
        ## skip if previously marked
        next if (666 == $lexicon_hash{$bestword}{line_list}[$_]->[0]);
        my $tmp_line = $lexicon_hash{$bestword}{line_list}[$_]->[1];
        if (exists $available_lines_hash{$tmp_line}){
            $lexicon_hash{$bestword}{line_list}[$_]->[0]
              = $available_lines_hash{$tmp_line}{score};
        }
        else {
            ## splicing is expensive; just move it out of the way.
            $lexicon_hash{$bestword}{line_list}[$_]->[0] = 666;
            #     ## this line is a ghost
            #     push @indices_to_prune, $_; # increasing order
            #     next;
        }
    } # foreach ( $updatesize..$#{$lexicon_hash{$bestword}{line_list}} )
    print STDERR "           pruning ", scalar @indices_to_prune,
      " ghosts.\n" if (@indices_to_prune);
    while (@indices_to_prune){
        my $prune = pop @indices_to_prune; # decreasing order!
        ## now it's safe to delete the already-used entries
        splice @{$lexicon_hash{$bestword}{line_list}}, $prune, 1;
    } # while @indices_to_prune
    ## skip word if no lines left
    $num_lines_for_bestword = count_num_lines_for_word(\$bestword);
    next if ($num_lines_for_bestword == 0);

print STDERR "         :: first update and prune "
 . Time::HiRes::tv_interval($t2) ."\n";
my $t2b = [Time::HiRes::gettimeofday];

    ## update the number of lines for the word.
    $num_lines_for_bestword = scalar @{$lexicon_hash{$bestword}{line_list}};
    ## reset updatesize
    $updatesize = 2*$batchsize;
    $updatesize = $num_lines_for_bestword
      if ($updatesize > $num_lines_for_bestword);

# print STDERR "update size: $updatesize, from $batchsize and "
#   .scalar @{$lexicon_hash{$bestword}{line_list}}." as scalar.\n";
# print STDERR "first element : ".$lexicon_hash{$bestword}{line_list}->[0][0]
#   ." and $updatesize: ".$lexicon_hash{$bestword}{line_list}->[$updatesize][0]
#   ."\n";

    ## find the best 2*batchsize lines in the stack
# {
# no autovivification qw(fetch exists strict); ## otherwise the sort fucks things up.
    $lexicon_hash{$bestword}{line_list} =
      [ Sort::Key::Top::nkeypart { $available_lines_hash{$_->[1]}{score} }
        $updatesize
        => grep { defined $available_lines_hash{@{$_}[1]} }
                @{$lexicon_hash{$bestword}{line_list}} ]; ## over {line_list} tuples
    # ## the top elements aren't themselves in sorted order, so:
    # @{$lexicon_hash{$bestword}{line_list}}[0..$updatesize-1]
    #   = sort { $a->[0] <=> $b->[0] }
    #     @{$lexicon_hash{$bestword}{line_list}}[0..$updatesize-1];
# }
    if ($save_memory
      && (defined $ultracommon_words_hash{$bestword})
      && ($num_lines_for_bestword > $maxcount) ){
        ## sort the head of the hash
        @{$lexicon_hash{$bestword}{line_list}}[0..$updatesize-1]
          = sort { $a->[0] <=> $b->[0] }
          @{$lexicon_hash{$bestword}{line_list}}[0..$updatesize-1];
        ## prune oversized ultracommon stacks back to $maxcount.
        $lexicon_hash{$bestword}{line_list}
          = [ @{$lexicon_hash{$bestword}{line_list}->[0..$maxcount-1]} ];
    }
#     ## find where the previous worst score wound up, but only look in the
#     ## just-updated pile?
# # TO FIX?
# #      = List::MoreUtils::firstidx { $_->[0] > $worst_batch_score }
#     my $insertion_index
#       = List::MoreUtils::lastidx { $_->[0] <= $worst_batch_score }
#       @{$lexicon_hash{$bestword}{line_list}}[0..$updatesize-1];
#     ## last_idx returns -1 if none found:
#     $insertion_index = $num_lines_for_bestword-1
#       if ($insertion_index < 0
#         or $insertion_index > $num_lines_for_bestword-1 );
    my $insertion_index = $updatesize - 1; ## speed hack
    print STDERR "    now updating everything up to line index ",
      "$insertion_index (out of ", ($num_lines_for_bestword - 1), " ) \n";
    ## update every element above it, unless we just updated it.
    foreach (0..$insertion_index) {
        my $tmp_line = $lexicon_hash{$bestword}{line_list}[$_]->[1];
        next if (exists $just_updated_lines{$tmp_line}
            and ($just_updated_lines{$tmp_line} > 0));
        my $tmp_sge = compute_sentence_gain_estimate(\$tmp_line);
        if (! defined $tmp_sge){
            ## this line is a ghost, which shouldn't happen, but...
            print STDERR "weirdness with $bestword and undefined line $tmp_line.\n";
            next;
        }
        my $tmp_score
            = $length_penalty{$available_lines_hash{$tmp_line}{tokencount}}
            + $tmp_sge;
        ## share information across stacks! use when sifting.
        $lexicon_hash{$bestword}{line_list}[$_]->[0] = $tmp_score;
        $available_lines_hash{$tmp_line}{score}      = $tmp_score
          if exists $available_lines_hash{$tmp_line}; ## paranoia
#print STDERR " score: ".$lexicon_hash{$bestword}{line_list}[$_]->[0]."\n";
    } # foreach (0..$insertion_index)
#    undef %just_updated_lines; ## clear the hash just to be sure.

print STDERR "         :: sort and update done in ". Time::HiRes::tv_interval($t2b) ."\n";
my $t2c = [Time::HiRes::gettimeofday];

# {
# no autovivification qw(fetch exists strict); ## otherwise the sort fucks things up.
    ## sort the stack (or part of it, anyway)
    $lexicon_hash{$bestword}{line_list} =
      [ Sort::Key::Top::nkeypart { $available_lines_hash{$_->[1]}{score} }
        $updatesize
        => grep { defined $available_lines_hash{@{$_}[1]} }
                @{$lexicon_hash{$bestword}{line_list}} ]; ## over {line_list} tuples
# }
    ## the top elements aren't themselves in sorted order, so:
    @{$lexicon_hash{$bestword}{line_list}}[0..$updatesize-1]
      = sort { $a->[0] <=> $b->[0] }
        @{$lexicon_hash{$bestword}{line_list}}[0..$updatesize-1];

# print STDERR "now index 0 element : ".$lexicon_hash{$bestword}{line_list}->[0][0]
#   ." and index ". ($updatesize - 1) .": ".$lexicon_hash{$bestword}{line_list}->[$updatesize-1][0]
#   ."\n";


    ## at this point, the best sentences are one whose scores we did just
    ## update, so the estimates are trustworthy. take the best batch off the
    ## front of {line_list}
    my @batchlines;
    ## note we're not removing this line from the line_lists for the other words it contains.
    if ($batchmode) {
        my @reinsert;
        my $prev_string = "";
        while (@{$lexicon_hash{$bestword}{line_list}}) {
            my $tuple = shift @{$lexicon_hash{$bestword}{line_list}}; ## first one is best-scoring one
            my ($line_score, $line_id) = @{$tuple};
            if ($available_lines_hash{$line_id}{string} eq $prev_string) {
                ## de-select string-duplicate consecutive lines.
                push @reinsert, [$line_score, $line_id];
#print STDERR " reinserting $line_id for $bestword\n";
                next;
            } else{
#print STDERR " pushing $line_id into batchlines for $bestword\n";
                push @batchlines, [$line_score, $line_id];
                $prev_string = $available_lines_hash{$line_id}{string};
                ## keep going til we hit the cap.
                last if scalar @batchlines >= $batchsize;
            }
        } # foreach  @{$lexicon_hash{$bestword}{line_list}}
        ## put the duplicates back
        unshift @{$lexicon_hash{$bestword}{line_list}}, @reinsert;
    } else {
        @batchlines = splice @{$lexicon_hash{$bestword}{line_list}}, 0, $batchsize;
    }
    print STDERR "    selecting ", scalar(@batchlines),
      " of $num_lines_for_bestword for JADED  \n";

print STDERR "         :: second sort and prep done in "
  . Time::HiRes::tv_interval($t2c) ."\n";


    print STDERR "     :: batch prep done in ". Time::HiRes::tv_interval($t2)
      . " // " . Time::HiRes::tv_interval($t0) ."\n";
    my $t3 = [Time::HiRes::gettimeofday];

    my %batchlines_count;
    ## now go through @batchlines and do the updating.
    while (@batchlines) {
        my $tuple = shift @batchlines; ## first one is best-scoring one
        my ($goodline_score, $goodline_id) = @{$tuple};
        if (! defined $available_lines_hash{$goodline_id}) {
            ## this line was already added, but we'd just spliced it
            ## out of @{$lexicon_hash{$bestword}{line_list}} anyway!
print STDERR "        ignored the ghost of goodline $goodline_id.\n";
            next; ## move on
        } # (! defined $available_lines_hash{$goodline_id})
        $currmodel_linecount++; ## give this line its new rank
        my @goodline_tokens
          = split(' ', $available_lines_hash{$goodline_id}{string});
        my %line_words;
        foreach (@goodline_tokens) {
            $line_words{$_}++;
            $batchlines_count{$_}++; ## count occurrences of each word type
        } # foreach (@goodline_tokens)
        foreach (keys %line_words){
            ## keep track of ultracommon_word usage
            $ultracommon_words_hash{$_}{lines_over}--
              if defined $ultracommon_words_hash{$_};
        }
        my $goodline_sge = $available_lines_hash{$goodline_id}{SGE};
        $currmodel_score += $goodline_score;
        ## JADED columns: sentence {input line_id, output rank, score,
        ## penalty, gain}, total score, the root word, WGE, and the squished line.
        print JADED join("\t", ( $goodline_id, $currmodel_linecount,
            $goodline_score, $length_penalty{scalar @goodline_tokens},
            $goodline_sge, $currmodel_score, $bestword,
            $selectable_words_hash{$bestword},
            $available_lines_hash{$goodline_id}{string} ) ) . "\n";

        ## update the current model wordcount & linecount
        $currmodel_wordcount += $available_lines_hash{$goodline_id}{tokencount};
        ## update the currmodel with the contents of the sentence.

        delete $available_lines_hash{$goodline_id}; ## remove the line entirely
        delete $ultracommon_lines_hash{$goodline_id}
          if defined $ultracommon_lines_hash{$goodline_id};
        $linecounter++;
        last if $linecounter == $numlines; ## ok to drop everything after this line.
        if ( $save_memory and ($linecounter % 1000000 == 0 )) {
            ## really expensive operation, do every millionth line.
            @ultracommon_lines_array = keys %ultracommon_lines_hash;
            print STDERR "    refreshing @ ultracommon_lines_array\n";
        }
    } # while @batchlines

    print STDERR "     :: batch printing done in " . Time::HiRes::tv_interval($t3)
      . " // " . Time::HiRes::tv_interval($t0) ."\n";
    my $t4 = [Time::HiRes::gettimeofday];

    ## %count now contains all the words from @goodlines put together
    while (my ($word, $word_count) = each %batchlines_count) {
        ## update the count for this word
        $currmodel_hash{$word}{count} += $word_count;
        ## decrement the still un-selected count for this word
        $lexicon_hash{$word}{avail}{count} -= $word_count;

        ## update the word gain estimates for these just-seen words.
        ## the WGE is unchanged for words not in the sentence.
        $lexicon_hash{$word}{WGE} = $lexicon_hash{$word}{hconstant}
          * log( ($currmodel_hash{$word}{count} + $smoothing_count)
            / ($currmodel_hash{$word}{count} + 1) );
        ## only update %selectable_words and the queue if it's already selectable.
        if (defined $selectable_words_hash{$word}){
            $selectable_words_hash{$word} = $lexicon_hash{$word}{WGE};
            ## update the weight of the word in the queue in O(log v)
            $selectable_words_queue->add($word, $lexicon_hash{$word}{WGE});
        }
    } #  while each %batchlines_count
    ## all newly-seen words updated.

    ## is this an ultracommon word with a depleted stack?
    if ( $save_memory
        && (defined $ultracommon_words_hash{$bestword})
        && ( (! defined $lexicon_hash{$bestword}{line_list})
             or ($num_lines_for_bestword < $refill_line)
        )){
        ## yep! fill'er up!
        print STDERR "refilling ' $bestword ' {line_list} from ",
          "$num_lines_for_bestword to $maxcount. ";
        ## empty out {line_list} so we can refill it properly.
        @{$lexicon_hash{$bestword}{line_list}} = ();
        ## refill, taking advantage of all updated scores since last refill.
        my @line_list
          = Sort::Key::Top::nkeytop { $available_lines_hash{$_}{score} } $maxcount
          => grep { (defined $ultracommon_lines_hash{$_})
                        and ($ultracommon_lines_hash{$_}{$bestword}) }
            @ultracommon_lines_array;
        ## we cache the keys infrequently because recomputing is expensive!
        # use line_ids to push scores and lines. requires pre-pruned array.
        foreach (@line_list) {
            push @{$lexicon_hash{$bestword}{line_list}},
                 [$available_lines_hash{$_}{score}, $_];
        }
        $ultracommon_words_hash{$bestword}{words_over}
          -= $batchlines_count{$bestword};
        ## is this word under the maxcount cap and no longer ultracommon?
        if ( ($ultracommon_words_hash{$bestword}{lines_over} < 0)
            or (scalar @line_list < $maxcount) ){
            print STDERR "\n   !! word $bestword is no longer ultracommon! "
              . "only appears in ". scalar @line_list ." more lines.\n";
            delete $ultracommon_words_hash{$bestword};
        }
        print STDERR " now ", count_num_lines_for_word(\$bestword),
          " lines available.\n";
    } # while each %ultracommon_lines_hash
    ## else: nope, this is a normal word

    ## check if we've exhausted this word and can trim the vocab.
    count_num_lines_for_word(\$bestword);

    ## update the length penalties for the next sentence we add
    update_length_penalties();
    $iterations--;
    $loopcounter++;

    print STDERR "     :: update done in ", Time::HiRes::tv_interval($t4),
      "\n     :: finished in ", Time::HiRes::tv_interval($t0), " sec total.\n";
    last if $linecounter == $numlines;
} # while ($iterations > 0)

print STDERR "to here: ", scalar (keys %available_lines_hash), " lines available\n";
print STDERR "to here: ", scalar (keys %selectable_words_hash), " selectable words\n";
print STDERR "to here: ", scalar (keys %ultracommon_lines_hash), " ultra lines\n";
print STDERR "to here: ", scalar (keys %ultracommon_words_hash), " ultra words\n";

## Good stuff done; deal with the junk lines now.
print STDERR "Selected all non-junk sentences! Adding the junk now...";
my $junk_counter=0;
while (my ($line_id, $throwaway_hashvalue) = each %junk_lines_hash) {
    $linecounter++;
    $currmodel_linecount++;
    $junk_counter++;
    ## JADED columns: sentence {input line_id, output rank, score,
    ## penalty, gain, total score, the root word, WGE, and the string.
    print JADED join("\t", ( $line_id, $currmodel_linecount, 666,
       666, 666, 666, "JUNKLINE", 666,
       $junk_lines_hash{$line_id}{string} ) ) . "\n";
    delete $junk_lines_hash{$line_id}; ## remove the line entirely
} # while each %junk_lines_hash
print STDERR "...done adding $junk_counter junk lines.\n Selected all sentences! Finished after $linecounter lines / $loopcounter iterations. \n\n";

print STDERR "to here: ", scalar (keys %available_lines_hash), " lines available\n";
print STDERR "to here: ", scalar (keys %selectable_words_hash), " selectable words\n";
print STDERR "to here: ", scalar (keys %ultracommon_lines_hash), " ultra lines\n";
print STDERR "to here: ", scalar (keys %ultracommon_words_hash), " ultra words\n";

print STDERR "=============================\n";
print STDERR "  Done selecting sentences.\n";
print STDERR "=============================\n";
print STDERR "Done at ", (localtime), "\n";


sub update_length_penalties {
    ## compute the penalties for adding a certain number of words to the
    ## selected set. The base case (where currmodel_wordcount=0) is handled above.
    foreach (0..$length_cap) {
    	$length_penalty{$_}
          = log( ($currmodel_wordcount + $_) / $currmodel_wordcount );
    }
}

sub compute_sentence_gain_estimate {
    ## compute the updated gain term from adding this line next.
    ## call as:
    ## my $sentence_gain_estimate = compute_sentence_gain_estimate(\$line_id);
    my ($ref_line_id, @bad) = @_;
    die "Extra args to compute_sentence_gain_estimate \n" if @bad;
    ## does the line still exist?
    return undef unless defined $available_lines_hash{$$ref_line_id};
    my @tokens = split(' ', $available_lines_hash{$$ref_line_id}{string});
    my $sentence_gain_estimate = 0;
    ## count occurrences of each word per line
    my %count;
    $count{$_}++ for (@tokens);
    while (my ($word, $throwaway_hashvalue) = each %count) {
    	## compute the estimated gain for the sentence
    	$sentence_gain_estimate += ($lexicon_hash{$word}{hconstant}
            * log( ($currmodel_hash{$word}{count} + $smoothing_count)
                    / ($currmodel_hash{$word}{count} + $count{$word}) ) );
    } # while
    ## update the sentence's estimated gain
    $available_lines_hash{$$ref_line_id}{SGE} = $sentence_gain_estimate;
    return $sentence_gain_estimate;
} # sub

sub find_best_word_gain_estimate {
    ## find single word with best/lowest WGE in O(v) time.
    ## call as:
    ## my $bestword = find_best_word_gain_estimate(\%selectable_words_hash, \$selectable_words_queue);
    my ($ref_selectable_words_hash, $ref_selectable_words_queue, @bad) = @_;
    die "Extra args to find_best_word_gain_estimate \n" if @bad;
    ## finds the key of hash with lowest value. see http://perldoc.perl.org/List/Util.html#reduce
    if ($$ref_selectable_words_queue->size() > 0) {
        ## the queue does this in O(1) time.
        my $sub_bestword = $$ref_selectable_words_queue->peek();
        return $sub_bestword;
    } else {
        ## check if we still have words to pick.
        print STDERR "Out of words! Going for another second pass with lower standards...\n";
        if (! %{$ref_selectable_words_hash}) {
            ## nope, no %selectable_words
            %selectable_words_hash = %secondpass_words_hash;
            %secondpass_words_hash = ();
            ## ok, _now_ check again if we have any words to pick.
            if (! %selectable_words_hash) {
                 ## nope, no %secondpass_words words either. quit!
                 print STDERR "No words left at all! $linecounter lines / $loopcounter iterations\n";
                 return undef;
            } # if (! %selectable_words_hash)
            ## if we get here, then we did just add secondpass words.
            print STDERR " * indexing sentences by the secondpass words...\n";
            print STDERR " * compute gain estimates for selectable words...   ";
            while (my ($word, $throwaway_hashvalue) = each %selectable_words_hash) {
                ## update the word gain estimates for all selectable words.
                ## the value is the WGE itself.
                $selectable_words_hash{$word} = $lexicon_hash{$word}{hconstant} *
                log( ($currmodel_hash{$word}{count} + $smoothing_count) /
                   ($currmodel_hash{$word}{count} + 1)   );
                ## copying it to %lexicon_hash just in case.
                $lexicon_hash{$word}{WGE} = $selectable_words_hash{$word};
                ## go building the priority queue (heap) for selectable words
                $selectable_words_queue->add($word, $lexicon_hash{$word}{WGE});
            } # while each %selectable_words_hash
            print STDERR "...done\n";
            print STDERR " * indexing sentences by their selectable vocabulary words...   ";
            while (my ($line_id, $throwaway_hashvalue) = each %available_lines_hash) {
                ## the gain term from adding this line next. total model
                ## improvement score (net benefit if negative, net harmful if
                ## positive) of a sentence is the penalty plus the gain.
                my $sentence_gain_estimate = compute_sentence_gain_estimate(\$line_id);
# do something if SGE is undef
                ## it's unnecessary to compute for all %available_lines, as we can get
                ## away with initially only computing for the lines that contain a
                ## selectable word. but... for now it's just initialization overhead.
                $available_lines_hash{$line_id}{SGE} = $sentence_gain_estimate;
                my @tokens = split(' ', $available_lines_hash{$line_id}{string});
                my $score = $length_penalty{scalar @tokens} + $sentence_gain_estimate;
                $available_lines_hash{$line_id}{score}=$score;
                ## count occurrences of each word per line
                my %count;
                $count{$_}++ for (@tokens);
                while (my ($word, $throwaway_hashvalue_innerloop) = each %count) {
                    ## now link each line plus its estimated gain to each word
                    ## i.e. associate this line with every *selectable word*
                    ## it contains. the restriction to selectable_words keeps
                    ## the hash size manageable.
                    next unless defined $selectable_words_hash{$word};
                    ## have to push an (anonymous) reference so that it doesn't
                    ## get flattened into one long array!
                    push @{$lexicon_hash{$word}{line_list}}, [$score, $line_id];
                } # while each %count
            } # while each %available_lines_hash)
            print STDERR "...done\n";
            print STDERR " * sorting lines for selectable words...  ";
            while (my ($word, $throwaway_hashvalue) = each %selectable_words_hash) {
                my $num_lines_for_word = count_num_lines_for_word(\$word);
                ## skip word if no lines left
                next if ($num_lines_for_word == 0);
                ## sort the lines for each word, in place, lowest score first.
                @{$lexicon_hash{$word}{line_list}}
                  = sort { $a->[0] <=> $b->[0] } @{$lexicon_hash{$word}{line_list}};
            } # if (%selectable_words_hash)
            print STDERR "...done adding secondpass_words to selectable_words\n";
# print STDERR "to here: ". scalar (keys %available_lines_hash) ." lines available\n";
# print STDERR "to here: ". scalar (keys %selectable_words_hash) ." selectable words\n";
# print STDERR "to here: ". scalar (keys %ultracommon_lines_hash) ." ultra lines\n";
# print STDERR "to here: ". scalar (keys %ultracommon_words_hash) ." ultra words\n";
        } # if (! %selectable_words_hash)
        my $sub_bestword = $$ref_selectable_words_queue->peek();
        return $sub_bestword;
    }
}

sub count_num_lines_for_word {
    ## count how many lines in {line_list} remain for $bestword.
    ## not the same as number of lines for words not in %selectable!
    ## call as:
    ##  my $num_lines_for_bestword = count_num_lines_for_word(\$bestword);
    my ($ref_bestword, @bad) = @_;
    die "Extra args to count_num_lines_for_word \n" if @bad;
#    print STDERR "counting lines for $$ref_bestword \n";
    if ( (! defined $lexicon_hash{$$ref_bestword}{line_list})
          or (scalar @{$lexicon_hash{$$ref_bestword}{line_list}} == 0) ){
        ## zero lines in the list
        @{$lexicon_hash{$$ref_bestword}{line_list}} = ();
        if ($save_memory
          and defined $ultracommon_words_hash{$$ref_bestword}){
            ## ultracommon needs refill, taking advantage of all updated
            ## scores since last refill.
            my @line_list
              = Sort::Key::Top::nkeytop { $available_lines_hash{$_}{score} } $maxcount
              => grep { (defined $ultracommon_lines_hash{$_})
                            and ($ultracommon_lines_hash{$_}{$$ref_bestword}) }
                @ultracommon_lines_array;
            # use line_ids to push scores and lines.
            foreach (@line_list) {
                push @{$lexicon_hash{$$ref_bestword}{line_list}},
                     [$available_lines_hash{$_}{score}, $_];
            }
            ## is this word under the maxcount cap and no longer ultracommon?
            if ( ($ultracommon_words_hash{$$ref_bestword}{lines_over} < 0)
                or (scalar @line_list < $maxcount) ){
                print STDERR "\n   !! word $$ref_bestword is no longer ultracommon! ",
                  "only appears in ", scalar @line_list, " more lines.\n";
                delete $ultracommon_words_hash{$$ref_bestword};
            }
            return scalar @line_list;
        }
        else {
            ## cleanup if no lines left (this is why it goes in a subroutine);
            ## delete the word from %selectable_words
            print STDERR "\n        no lines left for word $$ref_bestword :",
                " deleting it from selectable_words_hash. ";
            delete $selectable_words_hash{$$ref_bestword};
            $selectable_words_queue->remove($$ref_bestword);
            delete $ultracommon_words_hash{$$ref_bestword}
              if defined $ultracommon_words_hash{$$ref_bestword};
            print STDERR $selectable_words_queue->size(), " words left.\n";
            ## nuke the line_list entirely, just to be sure.
            return 0;
        } # if $save_memory and defined $ultracommon
    } else {
        ## count number of lines remaining
        return scalar @{$lexicon_hash{$$ref_bestword}{line_list}};
    } # if ( (! defined $lexicon_hash{$$ref_bestword}{line_list})
}

exit;

# to-do: consider ditching the keep_boring flag entirely.
# add threading?

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
