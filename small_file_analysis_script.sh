#!/bin/bash

# Script developed to Find the Number of Files Less then 250 MB in Hive Tables
# Developed by Anamika Singh.


export HOST_NAME=`hostname`

# Reading Arguments for database list and location
filename_with_path=$1
filename=$(basename "$filename_with_path")
filepath=`pwd`

case $HOST_NAME in
             host_name|host_name|host_name)
                     export EXEC_ENV="DEV"
                     export beeline='beeline -u "beeline string"'
             ;;
             host_name|host_name|host_name)
                     export EXEC_ENV="UAT"
                     export beeline='beeline -u "beeline string"'
             ;;
             host_name|host_name)
                     export EXEC_ENV="PROD"
                     export beeline='beeline -u "beeline string"'
             ;;
             *)
             echo "Unknown host: $host"
             ;;
esac

# Select modelname from filename and create directory under filepath to story Results
modelname=`cut -d_ -f1 <<< $filename`
`mkdir -p ${filepath}/${modelname}_smallfiles_result`
export model_dir="${filepath}/${modelname}_smallfiles_result"

# Reading Hive Database list:
#dbname=${filepath}/${filename}
dbname=${filename}
n=l

# Writing Field Headings into CSV File
echo -e "Hive Database Name, Table Name, HDFS Path, No. of Files less than 250MB" > ${model_dir}/${modelname}_Small_File_Count.csv

while read line; do
# reading each line
n=$((n+l))

# Collecting Table List from Hive Database:
$beeline -e "use $line; show tables;" | awk -F "| " '{print $2}' | grep . > ${model_dir}/${modelname}_table_list.txt

# Reading Hive Table List
tblname="${model_dir}/${modelname}_table_list.txt"

n1=l1
        while read line1; do
        # reading each line1
        n1=$((n1+l1))

        # Reading HDFS Path from Table
        export hdfs_path=`$beeline -e "DESCRIBE FORMATTED $line.$line1;" | grep 'Location' | awk -F "|" '{print $3}' | cut -d'/' -f4-`

        # Reading Count of Files Less then 250MB
        export count=`hdfs dfs -ls -R /${hdfs_path} | awk '$1 !~ /^d/ && $5 < 262144000 { print $8 }'|wc -l`

        # Writing Output Result into CSV File
        echo -e "${line}, ${line1}, /${hdfs_path}, ${count}" >> ${model_dir}/${modelname}_Small_File_Count.csv
        done < ${tblname}
done < ${dbname}

# Removing Zero File Count Records
awk '($5 > 0)' ${model_dir}/${modelname}_Small_File_Count.csv > ${model_dir}/${modelname}_Table_SmallFiles_Count.csv

# Sending Mail by Attaching Final List of CSV File with Count of File less than 250 MB
echo -e "Hello Team, \n \n Please find the attachment of No. of files less than 250MB for each Hive Table. \n \n \n Thanks & Regards, \n \n Data Analytics Team" | mailx -a ${model_dir}/${modelname}_Table_SmallFiles_Count.csv -s "${EXEC_ENV} - Automated - Hive Table File Size check"  -r "abc@mail" "test@mail.com"
                                                                                                                                                                            
rm -r ${model_dir}

echo "Small file analysis done and mail sent successfully"
