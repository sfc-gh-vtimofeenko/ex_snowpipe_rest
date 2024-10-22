import json
import argparse
import logging
import random
import string
import datetime

logging.basicConfig(level=logging.INFO)

# Defaults
chars = string.ascii_letters + string.digits
strlen_min = 50
strlen_max = 100
arylen_min = 5
arylen_max = 15
uni_min = -1000
uni_max = 1000
timeformat = '%Y-%m-%dT%I:%M:%p'
start_datetime = datetime.datetime(2024,1,1,0,0,0)
end_datetime = datetime.datetime(2024,7,1,0,0,0)


def read_file(fname):
    try:
        fp = open(fname, 'r')
        lines = fp.read().splitlines()
        return {x.split(':')[0]:x.split(':')[1] for x in lines}
    except Exception as e:
        logging.error(f"Error reading file ({fname}): ", e)
    
def gen_string():
    global strlen_min, strlen_max, chars
    len = random.randint(strlen_min, strlen_max)
    return ''.join(random.choices(chars, k=len))

def gen_array():
    global arylen_min, arylen_max
    len = random.randint(arylen_min, arylen_max)
    return [gen_string() for x in range(len)]

def gen_boolean():
    return random.choice([True,False])

def gen_float():
    global uni_min, uni_max
    return random.uniform(uni_min, uni_max)

def gen_datetime():
    global start_datetime, end_datetime, timeformat
    trange = end_datetime - start_datetime
    sdelta = random.randrange(int(trange.total_seconds()))
    return (start_datetime + datetime.timedelta(seconds = sdelta)).strftime(timeformat)

gen = {
    "VARCHAR": gen_string,
    "VARIANT": gen_string,
    "BOOLEAN": gen_boolean,
    "FLOAT": gen_float,
    "ARRAY": gen_array,
    "TIMESTAMP_NTZ": gen_datetime
}

def gen_data(args):
    global gen
    input = args.input
    output = args.output
    num_rows = args.num_rows
    fields = read_file(input)
    outf = open(output, 'w')

    for idx in range(num_rows):
        json.dump([{k:gen[v]() for k,v in fields.items()}], outf)
        outf.write('\n')


def main():
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--input', required=True, help='The input schema file name (each line is of the format "NAME:TYPE").')
    cli_parser.add_argument('--output', required=True, help='The output file name.')
    cli_parser.add_argument('--num_rows', type=int, default=10, help='Number of rows to generate.')
    args = cli_parser.parse_args()
    gen_data(args)

if __name__ == "__main__":
    main()
