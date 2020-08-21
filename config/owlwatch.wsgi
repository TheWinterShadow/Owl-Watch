activate_this='/var/www/Owl-Watch/.env/bin/activate_this.py'
with open(activate_this) as f:
	code=compile(f.read(), activate_this, 'exec')
	exec(code, dict(__file__=activate_this))

import os, sys, logging

sys.path.insert(0, '/var/www/Owl-Watch/app')
from run import app as application
