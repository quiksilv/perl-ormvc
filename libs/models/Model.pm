package Model;
use warnings;
use strict;

use lib "../datasources";
use parent 'TAppModel';
use DateTime;
use Data::Dumper;

sub new
{
	my ($class) = @_;
	my ($self) = TAppModel->new;
	$self->{'name'} = $class;
	$self->{'idColumnName'} = 'id';
	return bless $self, $class;
}
# length validation for data input based on column properties
sub validate
{
	my ($self, $parameters) = @_;
	my $validate;
	foreach($self->columns($self->get('tableName') ) ) {
		if($_->{'type'} eq "timestamp") {
			next;
		}
		if($_->{'name'} eq "id") {
			next;
		}
		if($_->{'nullable'} == 0 && $parameters->{$_->{'name'} } eq "") {
			$validate->{$_->{'name'} } = 'value cannot be null';
		}
		if($_->{'type'} eq "text" and length($parameters->{$_->{'name'} }) > ($_->{'precision'}-4) ) {
			$validate->{$_->{'name'} } = 'value cannot be longer than ' . $_->{'precision'} . ' characters';
		}
		if($_->{'type'} eq "int2") {
			if ($parameters->{$_->{'name'} } !~ m/^[+-]?\d+$/) {
				$validate->{$_->{'name'} } = 'value must be an integer';
			}
			if($parameters->{$_->{'name'} } > 32767 and $parameters->{$_->{'name'} } < -32768) {
				$validate->{$_->{'name'} } = 'value must be between -32768 and 32767';
			}
		}
		if($_->{'type'} eq "int4") {
			if ($parameters->{$_->{'name'} } !~ m/^[+-]?\d+$/) {
				$validate->{$_->{'name'} } = 'value must be an integer';
			}
			if($parameters->{$_->{'name'} } > 2147483647 and $parameters->{$_->{'name'} } < -2147483648) {
				$validate->{$_->{'name'} } = 'value must be between -2147483648 and 2147483647';
			}
		}
	}
	return $validate;
}
#provides INSERT and UPDATE query depending on if a record primary key id is supplied or not
sub save
{
	my ($self, $parameters, $options) = @_;
	my $result = 0;
	if(!defined $options->{'validate'}) {
		$options->{'validate'} = 0;
	}
	#for use with file caching
	$self->set('updated', time . rand(100) );
	if(!$parameters->{'id'}) {
		#INSERT query
		$parameters->{'created'} = DateTime->now();
	} else {
		#UPDATE query
		delete($parameters->{'created'});
		$self->set('id', $parameters->{'id'});
	}
	delete($parameters->{'id'});
	$parameters->{'updated'} = DateTime->now();
	if($options->{'validate'}) {
		#validate the inputs
		$self->set('error', $self->validate($parameters) ); #returns HASHREF
	}
	my $error = $self->get('error');
	if (!keys %{$error}) {
		$result = $self->SUPER::save($parameters); #returns numeric id of the record updated or inserted
	}
	return $result;
}

sub delete
{
	my ($self, $parameters) = @_;
	$self->set('updated', time . rand(100) );
	my $result = $self->SUPER::delete($parameters);
	return $result;
}
sub beforeSave
{
	my ($self, $parameters) = @_;
}

sub afterSave
{
	my ($self, $parameters) = @_;
}
1;
__END__
