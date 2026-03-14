# 
# this is the file which defines all the bands that are allowed in the system
#
# each entry can contain an arbitrary no of entries. 
#
# an entry can have an arbitrary no of PAIRS of frequencies, 
# these pairs attach themselves to the labels you provide, they are
# independant of any other pair, they can overlap, cross etc. 
#
# There MUST be at last a 'band' entry
#
# It is up to YOU to make sure that it makes sense!
#
# ALL the labels MUST BE in lower case.
# 


%bands = (
		  '73khz' => bless ( { band => [71, 75],
							 }, 'Bands'),
		      
		  '136khz' => bless ( { band => [135, 138],
							  }, 'Bands'),

		  '500khz' => bless ( { band => [493, 525],
							  }, 'Bands'),

		  '160m' => bless( { band => [ 1800, 2000 ], 
							 cw => [ 1800, 1830 ], 
							 rtty => [ 1838, 1841 ], 
							 ssb => [ 1831, 2000] 
						   }, 'Bands'),

		  '80m' => bless( { band => [ 3500, 4000 ], 
							cw => [ 3500, 3600 ], 
							data => [ 3580, 3619 ], 
							rtty => [ 3580, 3619 ], 
							sstv => [ 3730, 3740 ], 
							ssb => [ 3601, 4000 ]  
						  }, 'Bands'),

		  '60m' => bless( { band => [ 5100, 5410],
							ssb => [5300, 5410],
				}, 'Bands' ),

		  '40m' => bless( { band => [ 7000, 7400 ], 
							cw => [ 7000, 7040 ],
							data => [ 7040, 7100],
							rtty => [ 7040, 7060],
							ssb => [ 7050, 7400 ]
						  }, 'Bands'),

		  '30m' => bless( { band => [ 10100, 10150 ], 
							cw => [ 10000, 10140 ], 
							data => [ 10141, 10149 ] ,
							rtty => [ 10141, 10149 ] 
						  }, 'Bands'),
		  
		  '20m' => bless( { band => [ 14000, 14350 ], 
							cw => [ 14000, 14100 ], 
							ssb => [ 14101, 14350 ], 
							beacon => [ 14099, 14100 ],
							sstv => [ 14225, 14235 ],
							data => [ 14070, 14098, 14101, 14111 ],
							rtty => [ 14070, 14098, 14101, 14111 ],
						  }, 'Bands'),

		  '17m' => bless( { band => [ 18068, 18168 ], 
							cw => [ 18068, 18100 ], 
							ssb => [ 18111, 18168 ], 
							data => [ 18101, 18108], 
							rtty => [ 18101, 18108], 
							beacon => [ 18109, 18110] 
						  }, 'Bands'),

		  '15m' => bless( { band => [ 21000, 21450 ], 
							cw => [ 21000, 21150 ], 
							data => [ 21070, 21119 ], 
							rtty => [ 21070, 21119 ], 
							ssb => [ 21151, 21450] 
						  }, 'Bands'),

		  '12m' => bless( { band => [ 24890, 24990 ], 
							cw => [ 24890, 24930 ], 
							ssb => [ 24931, 24990], 
							rtty => [ 24920, 24929], 
							data => [ 24920, 24929], 
						  }, 'Bands'),


		  '10m' => bless( { band => [ 28000, 29700 ], 
							cw => [ 28000, 28198 ], 
							data => [ 28050, 28149, 29200, 29299 ], 
							rtty => [ 28050, 28149 ], 
							space => [ 29200, 29300 ],
							ssb => [ 28201, 29299, 29550, 29700] 
						  }, 'Bands'),

		  '6m' => bless( { band => [50000, 52000],
						   cw => [50000, 50100],
						   ssb => [50100, 50500],
						 }, 'Bands'),

		  '4m' => bless( { band => [70000, 70631],
						   cw => [70030, 70250],
						   ssb => [70030, 70250],
						 }, 'Bands'),

		  '2m' => bless( { band => [144000, 148000],
						   cw => [144000, 144150],
						   ssb => [144150, 144500]
						 }, 'Bands'),

		  '220' => bless( { band => [220000, 222000],
						  }, 'Bands'),

		  '70cm' => bless( { band => [430000, 450000],
							 cw => [432000, 432150],
							 ssb => [432150, 432500],
						   }, 'Bands'),

		  '23cm' => bless( { band => [ 1240000, 1325000],
							 cw => [1296000, 1296150],
							 ssb => [1296150, 1296800],
						   }, 'Bands'),

		  '13cm' => bless( { band => [2310000, 2450000],
							 cw => [2320100, 2320150],
							 ssb => [2320150, 2320800],
						   }, 'Bands'),

		  '9cm' => bless( { band => [3400000, 3475000],
							cw => [3400000, 3402000],
							ssb => [3400000, 3402000],
						  }, 'Bands'),

		  '6cm' => bless( { band => [5650000, 5850000],
							cw => [5668000, 5670000, 5760000, 5762000],
							ssb => [5668000, 5670000, 5760000, 5762000],
						  }, 'Bands'),

		  '3cm' => bless( { band => [10000000, 10500000],
							cw => [10368000,10370000, 10450000, 10452000],
							ssb => [10368000,10370000, 10450000, 10452000],
						  }, 'Bands'),

		  '12mm' => bless( { band => [24000000, 24250000],
							 cw => [24048000, 24050000],
							 ssb => [24048000, 24050000],
						   }, 'Bands'),
    
		  '6mm' => bless( { band => [47000000, 47200000],
							cw => [47087000, 47089000],
							ssb => [47087000, 47089000],
						  }, 'Bands'),
		  '4mm' => bless( { band => [75500000, 81000000], 
						  }, 'Bands'), 

		  'band1' => bless ( { band => [47000, 49999, 52000, 68000],
							 }, 'Bands'),
		      
		  'band2' => bless ( { band => [87500, 108000],
							 }, 'Bands'),
		      
		  'band3' => bless ( { band => [176000, 230000],
							 }, 'Bands'),
		      
		  'band4' => bless ( { band => [471000, 550000],
							 }, 'Bands'),
		      
		  'band5' => bless ( { band => [550000, 868000],
							 }, 'Bands'),

		  'military' => bless ( { band => [29700, 50000, 230000, 420000],
								}, 'Bands'),
		    
		  'aircraft' => bless ( { band => [108000, 137500],
								}, 'Bands'),
   
		  'pmrlow' => bless ( { band => [68000, 87500],
							  }, 'Bands'),
		      
		  'pmrmid' => bless ( { band => [138000, 165000],
							  }, 'Bands'),

		  'pmrhigh' => bless ( { band => [165000, 174000],
							   }, 'Bands'),

		  'pmruhf' => bless ( { band => [425000, 430000, 440000, 471000],
							  }, 'Bands'),
		 );

#
# the list of regions
#
# this list is so that users can say 'vhf/ssb' instead of '6m/ssb, 4m/sbb, 2m/ssb'
# just shortcuts really
#
# DO make sure that the label exists in %bands!
#

%regions = (
			vlf => [qw( 73khz 136khz 500khz )],
			hf => [qw( 160m 80m 60m 40m 30m 20m 17m 15m 12m 10m )],
			contesthf => [qw( 160m 80m 40m 20m 15m 10m )],
			vhf => [qw( 6m 4m 2m 220 )],
			vhfradio => [qw( band1 band2 )],
			vhftv => [qw( band1 band3 )],
			uhf => [qw( 70cm 23cm )],
			uhftv => [qw( band4 band5 )],
			shf => [qw( 23cm 13cm 9cm 6cm 3cm )],
			pmr => [qw( pmrlow pmrmid pmrhigh pmruhf )],
			spe => [qw( 10m 6m 4m 2m )],
			warc => [qw( 60m 30m 17m 12m )],
			all => [qw( 73khz 136khz 160m 80m 60m 40m 30m 20m 17m 15m 12m 10m 6m 4m 2m 220 70cm 23cm 9cm 6cm 3cm 12mm 6mm )],
		   );  

