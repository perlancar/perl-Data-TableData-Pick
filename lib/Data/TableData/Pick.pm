package Data::TableData::Pick;

use 5.010001;
use strict;
use warnings;

# AUTHORITY
# DATE
# DIST
# VERSION

use Exporter qw(import);
our @EXPORT_OK = qw(pick_table_rows);

our %SPEC;

$SPEC{pick_table_rows} = {
    v => 1.1,
    summary => 'Pick randomly one or more table rows, with some options',
    description => <<'_',

This function takes `table`, a table data (either aos, aoaos, aohos, or a
<pm:Data::TableData::Object> instance) and picks one or more random rows from it
and return the rows in the form of of aoaos or aohos.

No duplicates are picked (i.e. no resampling), but of course duplicate rows can
still happen if the input table itself contain duplicate rows.

It uses a modified version (to account for weights) of the algorithm in
<pm:Array::Pick::Scan>, which in turn uses a slightly modified version of
algorithm described in L<perlfaq> (L<perldoc -q "random line">)).

If the requested number of rows (`n`) exceed the number of rows of the table,
only up to the number of rows of the table are returned.

Weighting option. You can specify the name of column that contains weight.

_
    args => {
        table => {
            summary => 'A table data (either aos, aoaos, aohos, or a Data::TableData::Object instance)',
            schema => 'any*',
            req => 1,
        },
        n => {
            summary => 'Number of rows to pick',
            schema => 'posint*',
            default => 1,
        },
        weight_column => {
            summary => 'Specify column name that contains weight',
            schema => 'str*',
            description => <<'_',

If not specified, all rows will have the equal weight of 1.

Weight must be a non-negative real number.

_
        },
    },
};
sub pick_table_rows {
    require Data::TableData::Object;

    my %args = @_;
    my $weight_column = $args{weight_column};
    my $n = $args{n} // 1;

    my $td = Data::TableData::Object->new($args{table});
    my $iter = $td->iter;

    # we iterate twice, first to calculate sum of inverse weight for all rows
    my $sum_of_inv_weights_all_rows = 0;
    while (defined(my $row = $iter->())) {
        my $weight = defined $weight_column ?
            (ref($row) eq 'ARRAY' ? $row->[$weight_column] :
             ref($row) eq 'HASH' ? $row->{$weight_column} :
             die("Row is not array/hash")) : 1;
        my $inv_weight = $weight == 0 ? 0 : 1/$weight;
        $sum_of_inv_weights_all_rows += $inv_weight;
    }

    $iter = $td->iter;

    my @items; # each element is [$row, $inv_weight]
    my $sum_of_inv_weights_in_items = 0;
    my $sum_of_inv_weights_iterated = 0;
    while (defined(my $row = $iter->())) {
        #use DD; dd $row;
        my $weight = defined $weight_column ?
            (ref($row) eq 'ARRAY' ? $row->[$weight_column] :
             ref($row) eq 'HASH' ? $row->{$weight_column} :
             die("Row is not array/hash")) : 1;
        die "Weight cannot be negative ($weight)" if $weight < 0;
        my $inv_weight = $weight == 0 ? 0 : 1/$weight;
        my $item = [$row, $inv_weight];

        if (@items < $n) {
            #say "D: filling items";
            # we haven't reached $n, insert item to array in a random position
            splice @items, rand(@items+1), 0, $item;
            $sum_of_inv_weights_in_items += $inv_weight;
        } else {
            # we have reached $n, just replace an item randomly, using algorithm
            # from Learning Perl, modified to account for weights:

            # 1. should we replace?
            #say "D: should replace? rand($sum_of_inv_weights_iterated + $inv_weight) < $sum_of_inv_weights_in_items?";
            if (rand($sum_of_inv_weights_iterated + $inv_weight) < $sum_of_inv_weights_in_items) {
                #say "D:  yes, replace";

                # 2. pick a number between 0 <= x < $sum_of_inv_weights_in_items
                my $x = rand() * $sum_of_inv_weights_in_items;

                # 3. scan @items and find the right item (at the correct
                # position) to remove
                my $y = 0;
                for my $i (0 .. $#items) {
                    my $item2 = $items[$i];
                    my $y2 = $y + $item2->[1];
                    if ($x >= $y && $x < $y2) {
                        $sum_of_inv_weights_in_items -= $item2->[1];
                        splice @items, $i, 1, $item;
                        $sum_of_inv_weights_in_items += $item ->[1];
                        last;
                    }
                    $y = $y2;
                }
            }
        }
        $sum_of_inv_weights_iterated += $inv_weight;
    }

    return [map {$_->[0]} @items];
}

1;
# ABSTRACT:

=head1 SEE ALSO

L<Data::TableData::Object>

L<Array::Pick::Scan>
