#!/bin/sh

export ENV_INPUT_DIR_NAME=./sample.d
export ENV_TABLE_NAME=sample_dir_as_table

geninput(){
	echo generating sample data...

	mkdir -p "${ENV_INPUT_DIR_NAME}"

	echo hw1 > "${ENV_INPUT_DIR_NAME}/hw1.txt"
	echo hw2 > "${ENV_INPUT_DIR_NAME}/hw2.txt"
	touch "${ENV_INPUT_DIR_NAME}/empty.txt"
	mkdir -p "${ENV_INPUT_DIR_NAME}/empty.d"
}

test -d "${ENV_INPUT_DIR_NAME}/empty.d" || geninput

./dir2df2sql "
	SELECT
		filepath,
		is_dir,
		is_file,
		is_symlink,
		len,
		created,
		modified
	FROM sample_dir_as_table
	WHERE
		is_dir=false
		AND is_symlink=false
	ORDER BY len DESC
"
