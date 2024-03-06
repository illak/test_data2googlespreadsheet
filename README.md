## Pasos para construir la imágen y ejecutar el script

### Construir imagen

```
sudo docker run -it --rm data2spreadsheet:v01
```

Cada vez que se modifique el código debemos volver a ejecutar
el comando anterior. También debemos corroborar que no queden
imágenes *"dangling"*. Esto último lo podemos chequear de la
siguiente manera:

```
sudo docker images --filter dangling=true
```

En caso de existir imágenes en ese estado y de que queramos
borrarlas:

```
sudo docker rmi `sudo docker images --filter dangling=true -q`
```

### Ejecutar script

El script realiza consultas a la BD y va generando distintas
tablas que conforman un esquema de tipo "estrella". Una forma
de modelado que es adoptado por *data warehouses* relacionales.

Estas tablas se guardan en una planilla de google (Spreadsheets).

El script utiliza librerias de conexión a la BD y para la API
de Google Spreadsheets.

Para ejecutar el script luego de construir la imágen:

```
sudo docker run -it --rm data2spreadsheet:v01
```