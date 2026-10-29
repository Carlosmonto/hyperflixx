# Hadoop local para Spark en Windows

Este directorio contiene `bin/winutils.exe`, requerido por Hadoop/Spark para
operaciones locales de archivos en Windows. El job lo configura únicamente para
el proceso de Python; no modifica las variables de entorno del sistema.

El binario se obtiene del proyecto `cdarlint/winutils`, rama `hadoop-3.3.5`,
que publica binarios de Windows para Hadoop. Se conserva junto al proyecto para
que la ejecución sea reproducible.

Origen: `https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe`

SHA-256 verificado: `A0CA6E358357C41EF56EBDB02C38E4A4D55DA7CA7A13001678BB2EF7D644ADEA`
