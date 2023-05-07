
paths=('/external_databases' '/external_databases/data_adventure' '/external_databases/data_adventure/address'
       '/external_databases/data_adventure/customer' '/external_databases/data_adventure/customeraddress'
       '/external_databases/data_adventure/product' '/external_databases/data_adventure/productcategory' 
       '/external_databases/data_adventure/productmodel' '/external_databases/data_adventure/productmodeldescription'
       '/external_databases/data_adventure/salesorderdetail' '/external_databases/data_adventure/salesorderhead'
       '/databases' '/databases/data_adventure')

for path in "${paths[@]}"
do
    # Verifica se o caminho é um diretório
    hadoop fs -test -d "$path"
    if [ $? -eq 0 ]; then
        echo "$path é um diretório."
    else
        echo "$path não é um diretório."
        # Cria o diretório
        hadoop fs -mkdir "$path"
        echo "Diretório $path criado."
    fi
done


#create folder to save external databases
#hadoop fs -mkdir /external_databases
#hadoop fs -mkdir /external_databases/data_adventure


#create folder to receive csv files
#hadoop fs -mkdir /external_databases/data_adventure/address
#hadoop fs -mkdir /external_databases/data_adventure/customer
#hadoop fs -mkdir /external_databases/data_adventure/customeraddress
#hadoop fs -mkdir /external_databases/data_adventure/product
#hadoop fs -mkdir /external_databases/data_adventure/productcategory
#hadoop fs -mkdir /external_databases/data_adventure/productmodel
#hadoop fs -mkdir /external_databases/data_adventure/productmodeldescription
#hadoop fs -mkdir /external_databases/data_adventure/salesorderdetail
#hadoop fs -mkdir /external_databases/data_adventure/salesorderhead

#send csv files to folders
hadoop fs -put /dados_adventure/address.csv /external_databases/data_adventure/address
hadoop fs -put /dados_adventure/customer.csv /external_databases/data_adventure/customer
hadoop fs -put /dados_adventure/customeraddress.csv /external_databases/data_adventure/customeraddress
hadoop fs -put /dados_adventure/product.csv /external_databases/data_adventure/product
hadoop fs -put /dados_adventure/productcategory.csv /external_databases/data_adventure/productcategory
hadoop fs -put /dados_adventure/productmodel.csv /external_databases/data_adventure/productmodel
hadoop fs -put /dados_adventure/productmodeldescription.csv /external_databases/data_adventure/productmodeldescription
hadoop fs -put /dados_adventure/salesorderdetail.csv /external_databases/data_adventure/salesorderdetail
hadoop fs -put /dados_adventure/salesorderhead.csv /external_databases/data_adventure/salesorderhead

#databases
#hadoop fs -mkdir /databases
#hadoop fs -mkdir /databases/data_adventure
