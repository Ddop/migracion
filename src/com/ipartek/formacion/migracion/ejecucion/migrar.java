package com.ipartek.formacion.migracion.ejecucion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.PreparedStatement;

import com.ipartek.formacion.migracion.conexion.conexion;

public class migrar {

	/**
	 * La clase migrar trabaja con una ruta estática que debemos hardcodear. En
	 * dicha ruta depositaremos el fichero a migrar.
	 * 
	 * En dicha ruta se creará también un informe de resultado de la migración.
	 * Si fuese oportuno, se creará también un archivo con las insert que no han
	 * podido realizarse en la migración por tener una longitud excesiva para
	 * nuestras columnas.
	 * 
	 * static final String PATH: INDICAR AQUÍ RUTA ABSOLUTA DONDE ESTÁ EL TXT
	 * QUE QUIERES IMPORTAR static final String FICHERO: Nombre del fichero a
	 * importar, siempre en la ruta PATH. static final String FICHERO_RES:
	 * Nombre del fichero de resultado. static final String FICHERO_SQL: Nombre
	 * del fichero de insert que no han podido hacerse. (No tiene porque
	 * generarse)
	 */

	static final String PATH = "C://Desarrollo//Workspace//migracion//fichero//";
	// Es la ruta de trabajo para el fichero de entrada a migrar y los de salida
	static final String FICHERO = "personas.txt";
	// Es el fichero a migrar
	static final String FICHERO_RES = "resultado_migracion.txt";
	// Es el informe de salida
	static final String FICHERO_SQL = "sql_conflictivas.txt";
	// Es el archivo de SQL conflictivas, que se crea de existir alguna

	// Esta aplicación de migrado es un unico main ejecutable.
	public static void main(String[] args) {

		FileReader fr = null;
		BufferedReader br = null;
		String linea = null;

		int contador_si = 0;// Cuenta registros persistidos
		int contador_no = 0;// Cuenta registros que no se persisten por no venir
							// en el formato debido
							// (7 campos separados por coma)
		int contador_fallos = 0;// Cuenta las INSERT incompatibles con BBDD por
								// longitud
		int contador_columnas = 0;// Cuenta las columnas incompatibles con BBDD
									// por longitud

		boolean autorizado;
		// flag para los casos en que no sea compatible con las columnas de BBDD
		// a false, no deja insertar

		try {
			// SE INICIA LA LOGICA DEL MIGRADO
			fr = new FileReader(PATH + FICHERO);
			br = new BufferedReader(fr);

			// 0// Abrimos una conexión e iteramos mientras haya linea
			conexion con = new conexion();
			prepararArchivo(); // creamos el fichero de SQLs incompatibles en
								// blanco
			prepararResumen();
			while ((linea = br.readLine()) != null) {

				try {

					// la troceamos para trabajar con sus campos
					String[] aCampos = linea.split(",");

					// 1// SOLO ATENDEMOS FILAS CORRECTAS (7 campos)
					if (aCampos.length == 7) {
						autorizado = true;
						// No nos responsabilizamos de datos corruptos ni
						// incompletos, ni nada, todo lo que tenga formato 
						// de 7 campos, lo cogemos. Estamos migrando, no nos
						//podemos responsabilizar de que los datos sean reales,
						//etc

						String sql = "INSERT INTO `iparsex`.`persona` (`nombre`, `dni`, `observaciones`, `mail`) VALUES (?, ?, ?, ?);";
						PreparedStatement pst = con.getConnection().prepareStatement(sql);

						// 2// LOS CAMPOS TROCEADOS LOS EMBEBEMOS EN LA SQL
						// Miramos su longitud

						// Nombre, concatenado de 3 campos
						String nombre = aCampos[0] + " " + aCampos[1] + " " + aCampos[2];
						pst.setString(1, nombre);
						if (nombre.length() >= 51) {
							contador_columnas++;
							autorizado = false;
						}
						// DNI
						pst.setString(2, aCampos[5]);
						if (aCampos[5].length() >= 10) {
							contador_columnas++;
							autorizado = false;
						}
						// Observaciones
						pst.setString(3, aCampos[6]);
						if (aCampos[6].length() >= 251) {
							contador_columnas++;
							autorizado = false;
						}
						// Mail
						pst.setString(4, aCampos[4]);
						if (aCampos[4].length() >= 51) {
							contador_columnas++;
							autorizado = false;
						}

						// Miramos si algún campo ha dado problema de longitud,
						// Si el flag es true, 
						if ( autorizado == true ){
							//,lanzamos la insert, siendo el resultado insertado de 1,
							//y solo 1, la propia condición para contar un éxito
							if (pst.executeUpdate() == 1)
							contador_si++;// Es el contador de éxitos para hacer
											// el informe
							// Si no, archivamos sql
						} else {
							contador_fallos++;
							// String query = ParseString(pst);
							archivo(pst);
						}
						// 3// TRAS LANZAR SQL, CERRAMOS CONEXIÓN
						pst.close();

					} else {
						contador_no++;// Es el contador de fracasos para hacer
										// el informe
					}

					 System.out.println("Lineas migradas: "+contador_si);
					 System.out.println("Lineas que no cumplen formato: "+contador_no);
					 System.out.println("Lineas demasiado largas para insertar: "+contador_fallos);

				} catch (Exception e) {
					e.printStackTrace();
					// Atentos por si peta el splitting, pero pasamos
				}

			} // Fin del while

			con.desconectar();

		} catch (Exception e) {
			e.printStackTrace();
			// Pasamos. Solo es una migración

		} finally {
			// 4// AHORA CERRAMOS LA CONEXIÓN A FICHERO, no solo se cierra la
			// BBDD.
			try {
				if (br != null) {
					br.close();
				}

				if (fr != null) {
					fr.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
				// Pasamos. Solo es una migración
			}

		}

		// 5//HEMOS MIGRADO. AHORA CREAMOS UN FICHERO DE INFORME junto al
		// fichero
		// de donde hemos cogido los datos a migrar (PATH_FICHERO)
		fichero_resumen(contador_si, contador_no, contador_fallos, contador_columnas);
		// lo tenemos en una función private void

	}// Fin main

	/**
	 * Esta función crea un fichero resumen del proceso
	 * 
	 * @param int
	 *            contador_si, contador de registros importados correctamente
	 * @param int
	 *            contador_no, contador de registros que no se importan por no
	 *            tner 7 campos
	 * @param int
	 *            contador_fallos, contador de registros que no se importan por
	 *            ser demasiado grandes para nuestras columnas
	 * @param int
	 *            contador_columnas, desglose del contador anterior, por cada
	 *            columna. Un registro solo suma 1 a contador_fallos, pero puede
	 *            tener entre 1 y N (campos) columnas comprometidas
	 * 
	 *            Lo que hace esta función es escribir un txt con estos datos,
	 *            siendo los dos últimos opcionales, por lo que solo los
	 *            mostrará si nos hemos encontrado con sqls que pretendían
	 *            insertar columnas demasiado grandes para nuestra BD.
	 */

	private static void fichero_resumen(int contador_si, int contador_no, int contador_fallos, int contador_columnas) {

		FileWriter fw = null;
		PrintWriter pw = null;

		try {
			fw = new FileWriter(PATH + FICHERO_RES);
			pw = new PrintWriter(fw, true);
			int total = contador_si + contador_no + contador_fallos;
			pw.println("-----------------MIGRACIÓN OK-----------------");
			pw.println(" ");
			pw.println("Se han importado " + contador_si + " registros correctos");
			pw.println("Se han descartado " + contador_no + " registros por venir en formato incorrecto");
			if (contador_fallos > 0) {
				pw.println(" ");
				pw.println("Además, " + contador_fallos
						+ " INSERTs no eran compatibles con el tamaño de sus columnas, por " + contador_columnas
						+ " campos");
				pw.println("Se ha creado un fichero con las insert problemáticas");
			}
			pw.println(" ");
			pw.println(" ");
			pw.println("El fichero migrado contenía " + total + " registros");

		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			try {
				if (pw != null) {
					pw.close();
				}
				if (fw != null) {
					fw.close();
				}
			} catch (IOException e) {

				e.printStackTrace();
			}

		}

	}// FIN de fichero_resumen

	private static void prepararArchivo() {
		// Simplemente borramos el fichero de arcivo de sql por si existe de
		// ejecuciones anteriores
		// Lo hacemos fuera del bucle ya que si no, estariamos borrandolo con
		// cada sql incompatible
		// que escribimos, y solo almacenariamos la última.
		File fichero = new File(PATH + FICHERO_SQL);
		if (fichero.exists()) {
			fichero.delete();
		}
	}
	
	private static void prepararResumen() {
		File fichero = new File(PATH + FICHERO_RES);
		if (fichero.exists()) {
			fichero.delete();
		}
	}
	

	// Simplemente escribimos con append true la preparedStatement en un fichero
	// para dejar constancia de lo que NO se ha migrado por ser demasiado para
	// nuestras columnas
	private static void archivo(PreparedStatement pst) throws IOException {

		// System.out.println(pst);
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new FileWriter(PATH + FICHERO_SQL, true));
			out.write(pst.toString());
			out.newLine();
			out.write("-----------------------------------------------------");
			out.newLine();
		} catch (IOException e) {
			// pasamos
		} finally {
			if (out != null) {
				out.close();
			}
		}

	}// Fin de archivo

}// EOF
