#!/usr/local/bin/python3

"""
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
    
    made by Romain Lannes as PhD  the 01/11/2017
"""

"""
dependency :
fastasplit (exonerate tools which can be find here : http://www.ebi.ac.uk/about/vertebrate-genomics/software/exonerate
or here https://github.com/nathanweeks/exonerate)
"""


import argparse
import subprocess
import os

import time
import re
from multiprocessing import Pool



parser = argparse.ArgumentParser(description='will make a blast all against all')
parser.add_argument('-i', help='input', required=True)

parser.add_argument('-db', help='database for the blast')

parser.add_argument('-out', help='outfile', required=True)
parser.add_argument('-th', help='n_thread', required=True, type=int)
parser.add_argument('-blast_', help='path to the blast bin default blastp', default='blastp')
parser.add_argument('-db_type', default='prot', type=str, help='default; prot')
parser.add_argument('-mkdb', help='makeblastdb path', default='makeblastdb')
parser.add_argument('-fasta_spl', help='path to fastasplit', default='fastasplit', type=str)

parser.add_argument('-other_args', help='other_argument')
parser.add_argument('-std_args', help='standart argument for blast for now restrict to 1 if set to one do : \
the standard aire procedure', type=int, default=1)

args = parser.parse_args()



def blast(args_tuple):
	"""Just launch the blast"""

	blast, query, database, output_file = args_tuple
	blast_type = blast.split('/')[-1]
	if blast_type == 'blastp':
		if args.std_args:
			if args.std_args == 1:
				cmd = '{} -query {} -db {} -out {} -evalue 1e-5\
				  -outfmt "6 qseqid sseqid evalue pident bitscore qstart qend qlen sstart send slen" -max_target_seqs 10000 -soft_masking true '.format(blast, query, database, output_file)

		else:
			cmd = '{} -query {} -db {} -out {} \
			 -outfmt "6 qseqid sseqid evalue pident bitscore qstart qend qlen sstart send slen" -max_target_seqs 10000 -max_hsps 1'.format(blast, query, database, output_file)

			if args.other_args:
				cmd += ' {}'.format(args.other_args)

		try:
			print(cmd)
			child = subprocess.Popen(cmd, shell=True)
			child.wait()
		except:
			raise
		else:
			return 0

	elif blast_type == 'blastn':
		cmd = '{} -query {} -db {} -out {} -evalue 1e-5\
				  -outfmt "6 qseqid sseqid evalue pident bitscore qstart qend qlen sstart send slen" -max_target_seqs 10000 -soft_masking true '.format(blast, query, database, output_file)

		if args.other_args:
			cmd += ' {}'.format(args.other_args)

		try:
			print(cmd)
			child = subprocess.Popen(cmd, shell=True)
			child.wait()

		except:
			raise
		else:
			return 0




# we create a working dir
list_of_file = os.listdir('.')
cpt = 0
working_dir = "Allagainstallwd_{}".format(cpt)

while working_dir in list_of_file:
	cpt += 1
	working_dir = working_dir = "Allagainstallwd_{}".format(cpt)

# dayabase directory
db_dir = working_dir + '/' + 'db_'
# fasta split directory
split_dir = working_dir + '/' + 'split_'
# result directory
res_dir = working_dir + '/' + 'res'

os.makedirs(db_dir)
os.makedirs(split_dir)
os.makedirs(res_dir)

# if database does not exist we create it
if not args.db:
	db = db_dir + '/db_b'
	# make the db
	print('{} -dbtype {} -hash_index -in {} -out {}'.format(args.mkdb, args.db_type, args.i, db))
	child = subprocess.Popen('{} -dbtype {} -hash_index -in {} -out {}'.format(args.mkdb, args.db_type, args.i, db),
	                         stdout=subprocess.DEVNULL, shell=True)
	child.wait()
else:
	db = args.db

# for the split we need to know how many sequence we have
num_seq = int(subprocess.check_output("grep -c '>' {}".format(args.i), shell=True).decode('utf-8'))

if args.th < num_seq:
	nb_split = args.th
else:
	nb_split = num_seq
print('input_file splitted: {}'.format(nb_split))
# split the fasta
child = subprocess.Popen('{} -f {} -o {} -c {}'.format(args.fasta_spl, args.i, split_dir, nb_split),
                         stdout=subprocess.DEVNULL, shell=True)
child.wait()

# making a list with the absolute path to each splited fasta
all_split = os.listdir(split_dir)
abs_path = os.path.abspath(split_dir)
all_split = [abs_path + '/' + element for element in all_split]


# for the pool.map we need a tuple by proccess
# each tuple contain the balst arguments
liste_args = list()
cpt = 0
for file in all_split:
	cpt += 1
	tuple_args = (args.blast_, file, db, res_dir + '/' + os.path.basename(file) + str(cpt))
	liste_args.append(tuple_args)
	
# define the pool
pool = Pool(processes=args.th)

# launch the process
out_code = pool.map(blast, liste_args)

# cat result
child = subprocess.Popen("cat {} > {}".format(res_dir+'/*', args.out), shell=True)
child.wait()

# remove working dir
subprocess.Popen("rm -rf {}".format(working_dir), shell=True)
