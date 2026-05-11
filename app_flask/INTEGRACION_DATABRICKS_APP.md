# Cómo exportar datos desde Databricks para la aplicación

La app Flask consume CSV exportados desde la zona `refined`.  
Revise especialmente estos archivos dentro de `app_flask/databricks/`:

- `exportar_refined_para_app.py`
- `registrar_tablas_refined.sql`
- `05_inferencia_nuevas_muestras.py`

Flujo recomendado:
1. ejecutar notebooks 00–05 en Databricks;
2. validar tablas `refined_*`;
3. exportar los CSV para la app;
4. copiar los CSV al directorio `app_flask/data/`;
5. ejecutar `app.py`.
