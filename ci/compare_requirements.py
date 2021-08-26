import subprocess

def compare_requirements(hash):
    
    # compare requirements.txt files from Git 
    command = f"git diff -U1000 {hash}:requirements.txt requirements.txt"
    command = command.split(' ')
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, error = process.communicate()

    # check if error returned
    if error != None:
        print("0")

    # process output
    output = output.decode('ascii')
    output = output.split('\n')
    output = [word.strip() for word in output]

    # reduce to batchkit-specific requirements
    start = output.index("### batchkit")
    end = output.index("### examples")
    batchkit_reqs = output[start+1:end]

    # compare requirements line by line
    removed = added = []
    for req in batchkit_reqs:
        if req == '':
            pass
        elif req[0] == "-":
            removed.append(req[1:])
        elif req[0] == "+":
            added.append(req[1:])
    modified = [req for req in added if req in removed]
    if modified == []:
        print("1")
    else:
        print("0")