
#variables paths
paths=('/external_databases' '/external_databases/data_adventure' '/external_databases/data_adventure/address'
       '/external_databases/data_adventure/customer' '/external_databases/data_adventure/customeraddress'
       '/external_databases/data_adventure/product' '/external_databases/data_adventure/productcategory' 
       '/external_databases/data_adventure/productmodel' '/external_databases/data_adventure/productmodeldescription'
       '/external_databases/data_adventure/salesorderdetail' '/external_databases/data_adventure/salesorderhead'
       '/databases' '/databases/data_adventure')

#loop to crate folders on hdfs
for path in "${paths[@]}"
do
    # check if directory exists
    hadoop fs -test -d "$path"
    if [ $? -eq 0 ]; then
        echo "$path ."
    else
        echo "$path folder already exists."
        # create directory
        hadoop fs -mkdir "$path"
        echo "Directory $path created."
    fi
done

#move files into hdfs
hadoop fs -put -f /dados_adventure/address.csv /external_databases/data_adventure/address
hadoop fs -put -f /dados_adventure/customer.csv /external_databases/data_adventure/customer
hadoop fs -put -f /dados_adventure/customeraddress.csv /external_databases/data_adventure/customeraddress
hadoop fs -put -f /dados_adventure/product.csv /external_databases/data_adventure/product
hadoop fs -put -f /dados_adventure/productcategory.csv /external_databases/data_adventure/productcategory
hadoop fs -put -f /dados_adventure/productmodel.csv /external_databases/data_adventure/productmodel
hadoop fs -put -f /dados_adventure/productmodeldescription.csv /external_databases/data_adventure/productmodeldescription
hadoop fs -put -f /dados_adventure/salesorderdetail.csv /external_databases/data_adventure/salesorderdetail
hadoop fs -put -f /dados_adventure/salesorderhead.csv /external_databases/data_adventure/salesorderhead

