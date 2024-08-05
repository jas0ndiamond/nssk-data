-- resets database state for testing setup
-- run with -f as root

drop user nssk@'%';
drop user nssk@'192.168.%.%';
drop user nssk@'9.9.1.%';

drop user nssk_admin@'%';
drop user nssk_admin@'192.168.%.%';
drop user nssk_admin@'9.9.1.%';

drop user nssk_backup@'%';
drop user nssk_backup@'192.168.%.%';
drop user nssk_backup@'9.9.1.%';

drop user nssk_import@'%';
drop user nssk_import@'192.168.%.%';
drop user nssk_import@'9.9.1.%';

drop database NSSK_COSMO;
drop database NSSK_FLOWWORKS;
drop database NSSK_CNV_RAINFALL;
