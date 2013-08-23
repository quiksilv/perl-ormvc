package TAppModel;

use strict;
use warnings;
use lib "../../controllers";
use lib "../../models";
use lib "../../includes";
use lib "../../config";
use lib "../../libs/models";
use lib "../../libs/datasources";
use lib "../../libs/templates";
use lib "../../libs";
use Carp qw/croak/;
use Data::Dumper;
use DBI;
use TSettings;
use lib 'libs/datasources';
use parent 'TShop';
use parent 'Gocardless';
use TDBI;
use CHI;
use Time::HiRes qw(gettimeofday tv_interval);
use Storable qw(store retrieve);

sub new {
        my ($class) = @_;
	my ($self) = {};
	$self->{'name'} = $class;
        $self->{'cache'} = CHI->new(
                driver => 'File',
		expires_variance => 0.25,
		expires => '1 day',
                root_dir => FILE_CACHE_LOCATION . "models"
        );
	bless $self, $class;
        return $self;
}
#attributes
sub set {
	my ($self, $key, $value) = @_;
	if($key eq "updated") {
		store({$self->{'name'} => $value}, FILE_CACHE_LOCATION . 'models/'.$self->{'name'});
	}
	$self->{$key} = $value;
}
sub get {
	my ($self, $key) = @_;
	if($key eq "updated") {
		eval {
			my $obj = retrieve(FILE_CACHE_LOCATION . 'models/'.$self->{'name'});
			$self->{'updated'} = $obj->{$self->{'name'} };
		}
	}
	return $self->{$key};
}
# construct real table name based on user input or default behaviour
# default behaviour is to use the Model name, lowercase it and make it plural
sub _getTableName {
	my ($self) = @_;
	my $table_name;
	if(!defined $self->{'tableName'}) {
		$table_name = lc($self->{'name'}) . "s";
	} else {
		$table_name = $self->{'tableName'};
	}
	return $table_name;
}
#gets the id column name of a table from the user or by default use id.
sub _getIdColumnName {
	my ($self) = @_;
	my $id_column_name;
	if(!defined $self->{'idColumnName'}) {
		$id_column_name = "id";
	} else {
		$id_column_name = $self->{'idColumnName'};
	}
	return $id_column_name;
}
#get last inserted id the postgres way
sub lastInsertedId {
	my ($self) = @_;
	my $table_name = &_getTableName;
	my $id_column_name = &_getIdColumnName;
	my %result;
	my $sql = "SELECT CURRVAL(pg_get_serial_sequence('$table_name','$id_column_name'))";
	%result = &query($self, $sql);
	return $result{0}{'currval'};
}
#insert and update, no validation, no joins, no transaction support
sub save {
	my ($self, $parameters) = @_;
	my $starttime = [gettimeofday];
	my $result = 0;
	my $sql;
	if($self->get('id') ) {
		$sql = $self->_update($parameters);	
	} else {
		$sql = $self->_insert($parameters);
	}
	$result = TDBI::execute_sql2($sql, 2, 1); #INSERT queries return the generated id
	my $value = $result->fetchrow_array;
	$self->_debug($starttime, $sql);
	return $value;
}
sub _insert {
	my $fields;
	my $values;
	my ($self, $parameters) = @_;
	my $tablename = $self->_getTableName;
	foreach my $p ( keys %{$parameters} ) {
		$fields .= $p . ",";
		$values .= "'" . $parameters->{$p} . "',";	
	}
	$fields = substr($fields, 0 , -1);
	$values = substr($values, 0 , -1);
	return "INSERT INTO $tablename ($fields) VALUES ($values) RETURNING " . &_getIdColumnName;
}
sub _update {
	my $key_value;
	my ($self, $parameters) = @_;
	my $tablename = $self->get('tableName');
	my $idColumnName = $self->get('idColumnName');
#	foreach my $p ( keys %{$parameters} ) {
#		$key_value .= "$p='" . $parameters->{$p} . "',";
#	}
#	$key_value = substr($key_value, 0 , -1);
	$key_value = join(",", map { "$_='$parameters->{$_}'" } keys %{$parameters});
	return "UPDATE $tablename SET $key_value WHERE $idColumnName=" . $self->get('id') . " RETURNING " . &_getIdColumnName;
}

sub delete {
	my ($self, $parameters) = @_;
	my $starttime = [gettimeofday];
	my $where;
	my $result = 0;
	my $tablename = &_getTableName;
	$where = "WHERE " . join(" AND ", map { "$_='$parameters->{'where'}{$_}'" } keys %{$parameters->{'where'} });
	my $sql = "DELETE FROM $tablename $where";
	if(TDBI::execute_sql2($sql, 2, 1) ) {
		$result = 1;
	}
	$self->_debug($starttime, $sql);
	return $result;
}

sub executeTopup {
	my ($self, $parameters) = @_;
	my $tshop = new TShop;
	return $tshop->topup($parameters);
}
sub gocardless_bill {
	my ($self, $params) = @_;
	my $gc = new Gocardless;
	$gc->bill($params);	
}
sub gocardless_confirm_resource {
	my ($self, $params) = @_;
	my $gc = new Gocardless;
	$gc->confirm_resource($params);
}
sub find {
	my ($self, $type, $parameters, $options) = @_;
	if(defined $self->{'datasource'}) {
                if($self->{'datasource'} eq 'tshop_api') {                                                                         
			if($type eq "products") {
                        	&findTshopProducts($self, $parameters);
			} elsif($type eq "pricelist") {
				&findTshopPricelist($self, $parameters);
			} elsif($type eq "topup") {
				&executeTopup($self, $parameters);
			}
                } 
	#defaults to Postgresql
	} else {
		if($type eq "all" or $type eq "") {
			$self->findAll($parameters, $options);
		} elsif($type eq "count") {
			$self->findCount($parameters, $options);
		} elsif($type eq "distinct") {
			$self->findDistinct($parameters, $options);
		}
	}
}
sub findBy
{
	my ($self, $column, $parameters, $options) = @_;
	return $self->findAll({
		'where' => $parameters,
		'fields' => [$column]
	}, $options);	
}
sub findTshopProducts {
	my ($self, $parameters) = @_;
	my $tshop = new TShop;
	return $tshop->msisdn_info($parameters);
}
sub findTshopPricelist {
	my ($self, $parameters) = @_;
	my $tshop = new TShop;
	return $tshop->pricelist($parameters);
}
sub columns
{
	my ($self, $table) = @_;
	my $sth = TDBI::columns($table);
	my @data;
	for (my $i = 0; $i < $sth->{NUM_OF_FIELDS}; $i++) {
		my $name = $sth->{NAME}->[$i];
		### Describe the NULLABLE value
		#my $nullable = ("No", "Yes", "Unknown")[ $sth->{NULLABLE}->[$i] ];
		my $nullable = $sth->{NULLABLE}->[$i];
		### Tidy the other values, which some drivers don't provide
		my $scale = $sth->{SCALE}->[$i];
		my $precision  = $sth->{PRECISION}->[$i]; #more recent standards call this COLUMN_SIZE
		my $type  = TDBI::type_info($sth->{TYPE}->[$i]);
	    
		push @data, {
			'name' => $name,
			'type' => $type,
			'precision' => $precision,
			'scale' => $scale,
			'nullable' => $nullable,
		};
	}
	return @data;
}
sub findDistinct {
	my ($self, $parameters, $options) = @_;
	if(!defined $options->{'cache'}) {
		$options->{'cache'} = 1;
	}
	my $starttime = [gettimeofday];
	my %data;
	my $model;
	my $fields;	
	my $where;
	my $groups;
	my $order;
	my $limit;
	my $offset;

	#handle parameters of the sql query
#	if (exists $parameters->{"model"}) { $model = $parameters->{"model"} };
	$model = &_getTableName;
	if (defined $parameters->{"fields"} ) { 
		foreach (@{ $parameters->{"fields"} }) {
			$fields .= $_ . ", ";
		}
		$fields = "distinct " . substr($fields, 0, -2); #gets rid of the extra comma at the end
	} else {
		$fields = "*";
	}
	#
	# structure of sql BETWEEN 
	# {'where' => 
	# 	{'between' => {
	# 			'field' => some field
	# 			'from' => date_from
	# 			'to' =>	date_to
	# 		} 
	# 	}
	# }
	# or just plain sql
	# {'where' => "field1 = something" }
	#
	if (defined $parameters->{"where"}) {
#		if($parameters->{"where"}{"between"}) {
#			$where = "WHERE $parameters->{'where'}{'between'}{'field'} BETWEEN $parameters->{'where'}{'between'}{'from'} AND $parameters->{'where'}{'between'}{'to'}";
#		} else {
			$where = "WHERE $parameters->{'where'}";
#		}
	}
	if (defined $parameters->{"group"}) {
		if (@{ $parameters->{"group"} }) { 
			foreach (@{ $parameters->{"group"} }) {
				$groups .= $_ . ", ";
			}
			$groups = "GROUP BY " . substr($groups, 0, -2); #gets rid of the extra comma at the end
		}
	}
	if (exists $parameters->{"order"}) { $order = "ORDER BY " . $parameters->{"order"} };
	if (exists $parameters->{"limit"}) { $limit = "LIMIT " . $parameters->{"limit"} };
	if (exists $parameters->{"offset"}) { $offset = "OFFSET " . $parameters->{"offset"} };
	my $sql = trim("SELECT $fields FROM $model $where $groups $order $limit $offset");
	$data{$model} = $self->{'cache'}->get($sql."/".$self->get('updated') ) if(CACHING && $options->{'cache'});
	if(!defined $data{$model}) {
		my $result_set = TDBI::execute_sql2($sql);
		my %row;
		#push table name into the result array
		$data{$model} = {};
		my $row_number = 0;

	#	while( my @result_rows = $result_set -> fetchrow_array() ) {
	#		$row{$row_number} = {
	#			'type' => $result_rows[0],
	#			'date_trunc' => $result_rows[1],
	#			'sum' => $result_rows[2]	
	#		};
	#		$row_number++;
	#	}
		
		while( my $result_rows = $result_set->fetchrow_hashref ) {
			$row{$row_number} = {};
			if(defined ($parameters->{"fields"}) ) {
				foreach my $v (@{ $parameters->{"fields"} }) {
					#if the column part of the query is complicated due to the usage of date_trunc, sum or any other functions, use AS keyword to simplify the code below will refer to the simplified version.
					if(index($v, " AS ") != -1) {
						my $label = trim(substr($v, index($v, " AS ") + 3 ) );
						$row{$row_number}{$label} = $result_rows->{$label};
					} else {
						$row{$row_number}{$v} = $result_rows->{$v};
					}
				}
				if($self->{'hasAndBelongsToMany'}) {
					foreach my $t (@{$self->{'hasAndBelongsToMany'} }) { 
						$row{$row_number}{$t} = &_hasAndBelongsToMany($self, $parameters)->{'query'};
					}
				}
			} else {
				$row{$row_number} = $result_rows;
			}
			$row_number++;
		}
		$data{$model} = \%row;
		$self->{'cache'}->set($sql."/".$self->get('updated'), $data{$model}) if(CACHING && $options->{'cache'});
	}
	$self->_debug($starttime, $sql);
	return %data;
}
sub findAll {
	my ($self, $parameters, $options) = @_;
	if(!defined $options->{'cache'}) {
		$options->{'cache'} = 1;
	}
	my $starttime = [gettimeofday];
	my %data;
	my $model = $self->get('name');
	my $table = $self->get('tableName');
	my $fields;	
	my $where;
	my $groups;
	my $order;
	my $limit;
	my $offset;

	#handle parameters of the sql query
#	if (exists $parameters->{"model"}) { $model = $parameters->{"model"} };
	if (defined $parameters->{"fields"} ) { 
		foreach (@{ $parameters->{"fields"} }) {
			$fields .= $_ . ", ";
		}
		$fields = substr($fields, 0, -2); #gets rid of the extra comma at the end
	} else {
		$fields = "*";
	}
	#
	# structure of sql BETWEEN 
	# {'where' => 
	# 	{'between' => {
	# 			'field' => some field
	# 			'from' => date_from
	# 			'to' =>	date_to
	# 		} 
	# 	}
	# }
	# or just plain sql
	# {'where' => "field1 = something" }
	#
	if (defined $parameters->{"where"}) {
#		if($parameters->{"where"}{"between"}) {
#			$where = "WHERE $parameters->{'where'}{'between'}{'field'} BETWEEN $parameters->{'where'}{'between'}{'from'} AND $parameters->{'where'}{'between'}{'to'}";
#		} else {
		if(ref($parameters->{"where"}) ne "HASH") {
			$where = "WHERE $parameters->{'where'}";
		} else {
			$where = "WHERE " . join(" AND ", map { "$_='$parameters->{'where'}{$_}'" } keys %{$parameters->{'where'} });
		}
#		}
	}
	if (defined $parameters->{"group"}) {
		if (@{ $parameters->{"group"} }) { 
			foreach (@{ $parameters->{"group"} }) {
				$groups .= $_ . ", ";
			}
			$groups = "GROUP BY " . substr($groups, 0, -2); #gets rid of the extra comma at the end
		}
	}
	if (exists $parameters->{"order"}) { $order = "ORDER BY " . $parameters->{"order"} };
	if (exists $parameters->{"limit"}) { $limit = "LIMIT " . $parameters->{"limit"} };
	if (exists $parameters->{"offset"}) { $offset = "OFFSET " . $parameters->{"offset"} };
	my $sql = trim("SELECT $fields FROM $table $where $groups $order $limit $offset");
	$data{$model} = $self->{'cache'}->get($sql."/".$self->get('updated')) if(CACHING && $options->{'cache'});
	if(!defined $data{$model}) {
		my $result_set = TDBI::execute_sql2($sql);
		my %row;
		#push table name into the result array
		$data{$model} = {};
		my $row_number = 0;

	#	while( my @result_rows = $result_set -> fetchrow_array() ) {
	#		$row{$row_number} = {
	#			'type' => $result_rows[0],
	#			'date_trunc' => $result_rows[1],
	#			'sum' => $result_rows[2]	
	#		};
	#		$row_number++;
	#	}
		while( my $result_rows = $result_set->fetchrow_hashref ) {
			$row{$row_number} = {};
			if(defined ($parameters->{"fields"}) ) {
				foreach my $v (@{ $parameters->{"fields"} }) {
					#if the column part of the query is complicated due to the usage of date_trunc, sum or any other functions, use AS keyword to simplify the code below will refer to the simplified version.
					if(index($v, " AS ") != -1) {
						my $label = trim(substr($v, index($v, " AS ") + 3 ) );
						$row{$row_number}{$label} = $result_rows->{$label};
					} else {
						$row{$row_number}{$v} = $result_rows->{$v};
					}

				}
			} else {
				$row{$row_number} = $result_rows;
			}
			if($self->get('hasMany') && $options->{'depth'} == 1)
			{
				foreach(keys %{$self->get('hasMany')})
				{
					$row{$row_number}{'hasMany'}{$_} = {$self->_hasMany({
						'join' => $self->get('hasMany')->{$_},
						'where' => {
							$self->get('idColumnName') => $result_rows->{$self->get('idColumnName')} 
						}
					}) };
				}
			}
			$row_number++;
		}
		$data{$model} = \%row;
		$self->{'cache'}->set($sql."/".$self->get('updated'), $data{$model}) if(CACHING && $options->{'cache'});
	}
	#turn on sql printout, global is defined in config/TSettings.pm
	$self->_debug($starttime, $sql);
	return %data;
}
sub _hasMany
{
	my ($self, $parameters) = @_;
	my $join = $parameters->{'join'}{'tableName'};
	my $joinCondition = $parameters->{'join'}{'connectBy'};
	my $order = "ORDER BY " . $parameters->{'join'}{'order'} if($parameters->{'join'}{'order'});
	my $limit = "LIMIT " . $parameters->{'join'}{'limit'} if($parameters->{'join'}{'limit'});
	my $hasMany = $self->get('hasMany');
	my $table = $self->_getTableName;

	my $hasManyWhere;
	if(keys %{$parameters->{'where'} })
	{
		$hasManyWhere = join(" AND ", map {"$table.$_='$parameters->{'where'}{$_}'" if($parameters->{'where'}{$_}) } keys %{$parameters->{'where'} });
	}
	$hasManyWhere = "AND " . $hasManyWhere if($hasManyWhere);
	my $sql = "SELECT $join.* from $table, $join WHERE $joinCondition $hasManyWhere $order $limit";
	my $result_set = TDBI::execute_sql2($sql);
	my %row;
	my $row_number = 0;
	while(my $result_rows = $result_set->fetchrow_hashref) {
		$row{$row_number} = $result_rows;
		$row_number++;
	}
	return %row;
}
#works for the tags table
sub _hasAndBelongsToMany {
	my ($self, $parameters) = @_;
	my $table = &_getTableName;
	my $sql;
	my $where;
	foreach my $othertable (@{&get($self, 'hasAndBelongsToMany') }) {
		$othertable = lc($othertable) . "s"; #tags
		my @sorted = sort ($table, $othertable); #consists of retailer, tags
		my $connectingTable = join("_", @sorted); #returns retailer_tag
		if(defined $parameters->{'where'}) {
			$where = "AND $table.$parameters->{'where'}";
		}
		$sql = "SELECT $table.name, $othertable.name FROM $table, $othertable, $connectingTable WHERE $connectingTable.retailer_id = $table.id AND $othertable.id = $connectingTable.tag_id $where";
	}
	my %result = &query($self, $sql); 
	return \%result;
}
sub findCount
{
	my ($self, $parameters, $options) = @_;
	if(!defined $options->{'cache'}) {
		$options->{'cache'} = 1;
	}
	my $starttime = [gettimeofday];
	my $model;
	my $fields;	
	my $where;
	my $groups;
	my $order;
	my $limit;

	#handle parameters of the sql query
	$model = &_getTableName;
	if (defined $parameters->{"fields"} ) { 
		foreach (@{ $parameters->{"fields"} }) {
			$fields .= $_ . ", ";
		}
		$fields = substr($fields, 0, -2); #gets rid of the extra comma and space at the end
	} else {
		$fields = "*";
	}
	if ($parameters->{"where"}) {
			$where = "WHERE $parameters->{'where'}";
	}
	if (defined $parameters->{"group"}) {
		if (@{ $parameters->{"group"} }) { 
			foreach (@{ $parameters->{"group"} }) {
				$groups .= $_ . ", ";
			}
			$groups = "GROUP BY " . substr($groups, 0, -2); #gets rid of the extra comma at the end
		}
	}
	if (exists $parameters->{"order"}) { $order = "ORDER BY " . $parameters->{"order"} };
	if (exists $parameters->{"limit"}) { $limit = "LIMIT " . $parameters->{"limit"} };
	my $sql = trim("SELECT count(*) FROM $model $where");
	my $count = $self->{'cache'}->get($sql."/".$self->get('updated') ) if(CACHING && $options->{'cache'});
	if(!defined $count) {
		#turn on sql printout, global is defined in config/TSettings.pm
		my $result_set = TDBI::execute_sql2($sql);
		$count = $result_set->fetchrow_hashref->{"count"};
		$self->{'cache'}->set($sql."/".$self->get('updated'), $count) if(CACHING && $options->{'cache'});
	}
	my %result = ( "count" => $count );
	$self->_debug($starttime, $sql);
	return %result;
}
#
#	for specific queries
#
sub query
{
	my ($self, $sql, $options) = @_;
	if(!defined $options->{'cache'}) {
		$options->{'cache'} = 1;
	}
	$sql = trim($sql);
	my $starttime = [gettimeofday];
	my %row;
	my %data;
	$data{"query"} = $self->{'cache'}->get($sql."/".$self->get('updated') ) if(CACHING && $options->{'cache'});
	if(!defined $data{"query"}) {
		my $result_set = TDBI::execute_sql2($sql);
		my $row_number = 0;
		while( my $results = $result_set->fetchrow_hashref ) {
			$row{$row_number} = {};
			while( (my $key, my $value) =  each %$results) {
				$row{$row_number}{$key}  = $value;
			}
			$row_number++;
		}
		$data{"query"} = \%row;
		$self->{'cache'}->set($sql."/".$self->get('updated'), $data{"query"}) if(CACHING && $options->{'cache'});
	}
	$self->_debug($starttime, $sql);
	return %data;
}
sub _debug
{
	my ($self, $starttime, $sql) = @_;
	TLog::log($sql."/".$self->get('updated') );
	if(SQL_DEBUG) {
		my ($package, $filename, $line) = caller(1);
		print "File: ", $filename, " Line: ", $line, ": $sql<br/>";
		print tv_interval($starttime)*1000 . "ms<br/>";
	};
}
sub trim
{
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}
1;
