#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import argparse

def ext_data(source):
	return pd.read_csv(source)

def data_manu(data):
	new_data=data.copy()
	new_data[['Month','Year']]=new_data['MonthYear'].str.split(' ', expand=True)
	new_data['Sex'] = new_data['Sex upon Outcome'].replace("Unknown", np.nan)
	new_data.drop(columns=['MonthYear', 'Sex upon Outcome'], inplace=True)
	return new_data

def load_data(data,target):
	data.to_csv(target)

if  __name__ == "__main__":
	parser=argparse.ArgumentParser()
	parser.add_argument('source', help='source csv')
	parser.add_argument('target', help='target csv')
	args=parser.parse_args()

	print("Begin...")
	df=ext_data(args.source)
	new_df=data_manu(df)
	load_data(new_df, args.target)
	print("Done!")