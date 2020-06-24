import os
from subprocess import DEVNULL, STDOUT, check_call

print(f"\n\n{'-'*20}OPCUA Client Gateway: Installing Requirements{'-'*20}")
print(f"\n We are Checking Requirements for you to run the OPCUA Client Gateway! :)\n\n PLEASE WAIT....")

#pip install progressbar
check_call(['cmd.exe', '/c', 'pip install progressbar'], stdout=DEVNULL, stderr=STDOUT)
import progressbar

#Installing pip requirements.txt
bar = progressbar.ProgressBar(maxval=100, \
widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
bar.start()
if os.system('cmd.exe /c "pip install  -r requirements.txt"')==0:
	print(f"\n\nAll pip requirements were correctly installed! :)")
else:
	print(f"\nOPS! Something went wrong trying to install requirements.")
bar.finish()
