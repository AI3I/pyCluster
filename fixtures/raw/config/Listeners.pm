#
# Copy this file to /spider/local and modify it to your requirements
#
#
# This file specifies which local interfaces and ports you will be
# listening on
#
# You can add as many as you like
#

package main;

use vars qw(@listen);

@listen = (
["0.0.0.0", 7300],
["0.0.0.0", 7373],
["0.0.0.0", 8000],
);
 
1;
