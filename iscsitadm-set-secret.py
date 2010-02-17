#! /usr/bin/python

import os
import pexpect
import sys

def communicate(child):
    if child.expect([pexpect.EOF, 'Enter secret:\s*']) == 0:
        return
    child.sendline(secret)
    if child.expect([pexpect.EOF, 'Re-enter secret:\s*']) == 0:
        return
    child.sendline(secret)
    child.expect(pexpect.EOF)

if len(sys.argv) != 3:
    sys.stderr.write('Usage: ' + sys.argv[0] + ' <secret> <target>\n')
    os._exit(1)

secret = sys.argv[1]
tgt = sys.argv[2]

child = pexpect.spawn('/usr/sbin/iscsitadm modify initiator --chap-secret ' + tgt)
child.logfile_read = sys.stderr
communicate(child)
child.close()
if child.status != 0:
    sys.stderr.write('iscsitadm failed with exit code:' + str(child.status) + '\n')
    os._exit(1)
