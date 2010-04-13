use strict;
use warnings;

use Test::More;

use_ok('App::Cosmic::Client');
use_ok('App::Cosmic::Server::Linux');

# _parse_raid_status tests
is_deeply(
    App::Cosmic::Client::_parse_raid_status(map { "$_\n" } split /\n/, <<'EOT'),
/dev/md2:
        Version : 00.90.03
  Creation Time : Tue Apr 13 08:58:35 2010
     Raid Level : raid1
     Array Size : 102272 (99.89 MiB 104.73 MB)
  Used Dev Size : 102272 (99.89 MiB 104.73 MB)
   Raid Devices : 2
  Total Devices : 2
Preferred Minor : 2
    Persistence : Superblock is persistent

  Intent Bitmap : Internal

    Update Time : Tue Apr 13 08:58:40 2010
          State : active
 Active Devices : 2
Working Devices : 2
 Failed Devices : 0
  Spare Devices : 0

           UUID : 2c94a8a5:d8d31dad:7c8eb542:3c127690
         Events : 0.4

    Number   Major   Minor   RaidDevice State
       0       8       64        0      active sync   /dev/sde
       1       8       80        1      active sync   /dev/sdf
EOT
    ,
    +{
        '/dev/sde' => 'active sync',
        '/dev/sdf' => 'active sync',
    },
    'parse mdadm --detail (sync)',
);
is_deeply(
    App::Cosmic::Client::_parse_raid_status(map { "$_\n" } split /\n/, <<'EOT'),
    Number   Major   Minor   RaidDevice State
       0       0        0        0      removed
       1       8      112        1      active sync   /dev/sdh

       2       8       96        -      faulty spare   /dev/sdg
EOT
    ,
    +{
        '/dev/sdh' => 'active sync',
        '/dev/sdg' => 'faulty spare',
    },
    'parse mdadm --detail (faulty spare)',
);
is_deeply(
    App::Cosmic::Client::_parse_raid_status(map { "$_\n" } split /\n/, <<'EOT'),
    Number   Major   Minor   RaidDevice State
       0       8       64        0      active sync   /dev/sde
       1       0        0        1      removed
EOT
    ,
    +{
        '/dev/sde' => 'active sync',
    },
    'parse mdadm --detail (with remove)',
);

done_testing;
