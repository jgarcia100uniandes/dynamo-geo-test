# Probando indices con dynamo geo 

- Proyecto MVN 
- [GeoLibrary](https://aws.amazon.com/es/blogs/mobile/geo-library-for-amazon-dynamodb-part-1-table-structure/)
- Código tomado de [dynamodb-geo](https://github.com/amazon-archives/dynamodb-geo)
- Los datos de ejemplo estan en park.tsv
- tiene 3 métodos de prueba en la clase com.mycompany.geomain.DynamoGeo, son 
-- crearTabla();
-- insertarDatosEjemplo();
-- buscarPunto();
- Requiere permisos dynamo para create table, put item y scan (queries)
- la librería compilada (dynamodb-geo) está en el directorio lib

