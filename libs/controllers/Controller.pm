package Controller;
use warnings;
use strict;
use Data::Dumper;
use Data::Dumper::HTML qw/dumper_html/;
use Data::Pageset;
use JSON;
use lib "../../config"; use TSettings;

sub new
{
        my ($class) = @_;
	my ($self) = {};
	bless $self, $class;
	$self->{'name'} = $class;
	#assign session variables into self
	my $cgi = new CGI;
	my $sid = $cgi->cookie("CGISESSID") || undef;
	my $session = new CGI::Session($sid);
	foreach my $keys ($session->param() ) {
		$self->{'session'}{$keys} = $session->param($keys);
	}
	my ($package, $filename, $line, $subroutine) = caller(1); #provides the child Controller
	my ($controller, $view) = split("::", $subroutine);
	$self->set('controller', $controller);
	$self->_loadModel();
        return $self;
}
sub _loadModel
{
	my ($self, $model) = @_;
	if($model) {
	} else {
		$model = $self->_singular($self->get('controller'));
	}
	eval ("use $model;");
	$self->{$model} = $model->new;
	$self->set('model', $model);
}
#simple singularizer, just remove the s at the end.
sub _singular
{
	my ($self, $plural) = @_;
	$plural =~ s/s$//;
	return $plural;
}
sub _queryString
{
	my ($self) = @_;
	my $deserialized = {};
	my $query_string = $ENV{'QUERY_STRING'};
	$query_string =~ s/[^a-zA-Z0-9_&=]//g;
	my @pairs = split("&", $query_string);
	foreach (@pairs) {
		my @kv = split("=", $_);
		$deserialized->{$kv[0]} = $kv[1];
	}
	return $deserialized;
}
#attributes
sub set
{
	my ($self, $key, $value) = @_;
	$self->{$key} = $value;
}
sub get
{
	my ($self, $key) = @_;
	return $self->{$key};
}
sub referrer
{
	my ($self) = @_;
	my $store = STORE;
	return $ENV{HTTP_REFERER} if($ENV{'HTTP_REFERER'} =~ m/${store}/);
}
sub render
{
	my ($self, $parameters, $type) = @_;
	if(!defined $type) {
		$type = "html";
	}
	my $view_template = Template->new({
		INCLUDE_PATH => [
			'templates/'. lc($self->get('controller') ),  #primary folder for user defined templates
			'libs/templates/scaffold', #default folder if the user defined template doesn't exist
			'libs/templates/elements' #contains templates for general use, e.g. pagination
		]
	}) || die "$Template::ERROR";
	if($self->get("flash_message") )
	{
		$parameters->{'flash_message'} = $self->get("flash_message");
	}
	if($type eq "html") {
		#if paginate is used, the following two hash entries will not be empty objects
		$parameters->{'paging'} = $self->get('pagination_data') || {};
		$parameters->{'query_string'} = $self->_queryString;
		$parameters->{'referrer'} = $self->referrer();
		my ($package, $filename, $line, $subroutine) = caller(1);
		my ($controller, $view) = split("::", $subroutine); #returns Controller::<subroutine>
		$view_template->process($view.'.html', $parameters);
	} elsif($type eq "json") {
		$view_template->process("json", {"json" => encode_json($parameters) } );
	}
}
sub paginate
{
	my ($self, $parameters) = @_;
	$self->set('pagination_data', Data::Pageset->new({
		'total_entries' => {$self->{$self->get('model')}->find('count')},
		'entries_per_page' => $parameters->{'entries_per_page'},
		'current_page' => $parameters->{'current_page'},
		'mode' => 'slide'
	}) );
}
sub index
{
	my ($self, $parameters) = @_;
	my $current_page = $parameters->{'current_page'} || 1;
	my $entries_per_page = $parameters->{'limit'} || 100;
	$self->paginate({
		'current_page' => $current_page,
		'entries_per_page' => $entries_per_page,
	});
	$self->render({
		'model' => $self->get('model'),
		'controller' => lc($self->get('controller') ),
		'view' => $parameters->{'view'}, 
		'columns' => [$self->{$self->get('model')}->columns($self->{$self->get('model')}->get('tableName') )],
		'data' => {$self->{$self->get('model')}->find('all', {
			'offset' => ($current_page-1) * $entries_per_page,
			'limit' => $entries_per_page
		} )}
	});
}
sub view
{
	my ($self, $parameters) = @_;
	$self->render({
		'model' => $self->get('model'),
		'controller' => lc($self->get('controller') ),
		'view' => $parameters->{'view'},
		'columns' => [$self->{$self->get('model')}->columns($self->{$self->get('model')}->get('tableName') )],
		'data' => {$self->{$self->get('model')}->find('all', {
			'where' => "id=" . $parameters->{'id'}
		} )}
	});
}

sub edit
{
	my ($self, $parameters) = @_;
	if($ENV{'REQUEST_METHOD'} eq "POST")
	{
		my $data;
		foreach($self->{$self->get('model')}->columns($self->{$self->get('model')}->get('tableName') ) )
		{
			$data->{$_->{'name'} } = $parameters->{'form_data'}{$_->{'name'} };
		}
		if(!$self->{$self->get('model')}->save($data, {'validate' => 1}) ) {
			$self->render({
				'model' => $self->get('model'),
				'controller' => $self->get('controller'),
				'view' => $parameters->{'view'},
				'columns' => [$self->{$self->get('model')}->columns($self->{$self->get('model')}->get('tableName') )],
				'data' => $data,
				'error' => $self->{$self->get('model')}->get('error')
			});
		} else {
			$self->set("flash_message", "Data saved.");
			if(!$parameters->{'id'})
			{
				$self->render({
					'model' => $self->get('model'),
					'controller' => $self->get('controller'),
					'view' => $parameters->{'view'},
					'columns' => [$self->{$self->get('model')}->columns($self->{$self->get('model')}->get('tableName') )],
					'data' => {}
				});
			}
			else 
			{
				$self->render({
					'model' => $self->get('model'),
					'controller' => $self->get('controller'),
					'view' => $parameters->{'view'},
					'columns' => [$self->{$self->get('model')}->columns($self->{$self->get('model')}->get('tableName') )],
					'data' => {$self->{$self->get('model')}->find('all', {
						'where' => "id=" . $parameters->{'id'}
					} )}
				});
			}
		}
	}
	else
	{
		if(!defined $parameters->{'id'})
		{
			$self->render({
				'model' => $self->get('model'),
				'controller' => $self->get('controller'),
				'view' => $parameters->{'view'},
				'columns' => [$self->{$self->get('model')}->columns($self->{$self->get('model')}->get('tableName') )],
				'data' => {}
			});
		}
		else 
		{
			$self->render({
				'model' => $self->get('model'),
				'controller' => lc($self->get('controller') ),
				'view' => $parameters->{'view'},
				'columns' => [$self->{$self->get('model')}->columns($self->{$self->get('model')}->get('tableName') )],
				'data' => {$self->{$self->get('model')}->find('all', {
					'where' => "id=" . $parameters->{'id'}
				} )}
			});
		}
	}
}

sub delete
{
	my ($self, $parameters) = @_;
	$self->{$self->get('model')}->delete({
		'where' => {
			'id' => $parameters->{'id'}
		}
	});
}
1;
__END__
