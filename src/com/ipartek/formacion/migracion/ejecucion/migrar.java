package com.ipartek.formacion.migracion.ejecucion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.PreparedStatement;


import com.ipartek.formacion.migracion.conexion.conexion;

public class migrar {

	/*
	 * INDICAR AQUÍ RUTA ABSOLUTA DONDE ESTÁ EL TXT QUE QUIERES IMPORTAR 
	 */
	
	static final String PATH_FICHERO = "C://Desarrollo//Workspace//migracion//fichero//personas.txt";
	static final String PATH_FICHERO_RES = "C://Desarrollo//Workspace//migracion//fichero//personas_migradas.txt";
		
	//Esta aplicación de migrado es un unico main ejecutable.
	public static void main(String[] args) {

		
		FileReader fr = null;
		BufferedReader br = null;
		String linea = null;
		
		int contador_si = 0;
		int contador_no = 0;
		try {
			//SE INICIA LA LOGICA DEL MIGRADO
			fr = new FileReader(PATH_FICHERO);
			br = new BufferedReader(fr);
		
			//0// Mientras exista (siguiente) linea, y no pasemos del tamaño
			while(  (linea=br.readLine())     !=null )  {
				//la troceamos para trabajar con sus campos
				String []aCampos = linea.split(",");
				
				//1// SOLO ATENDEMOS FILAS CORRECTAS (7 campos)
				if (aCampos.length == 7) {
					contador_si++;//Es el contador de éxitos para hacer el informe
					
					conexion con = new conexion();
					String sql = "INSERT INTO `iparsex`.`persona` (`nombre`, `dni`, `observaciones`, `mail`) VALUES (?, ?, ?, ?);"; 
					PreparedStatement pst = con.getConnection().prepareStatement(sql);
					
					//2// LOS CAMPOS TROCEADOS LOS EMBEBEMOS EN LA SQL 
					pst.setString(1, aCampos[0] + aCampos[1] + aCampos[2] );
					//nombre es un caso especial por que es concatenación de 3 campos
					pst.setString(2, aCampos[5] );
					pst.setString(3, aCampos[6] );
					pst.setString(4, aCampos[4] );
					pst.executeUpdate();		
					//3// TRAS LANZAR SQL, CERRAMOS CONEXIÓN
					pst.close();
			    	con.desconectar();
			    	//TODO Estamos abriendo y cerrando conexiones por cada registro
					
				}
				
				contador_no++;//Es el contador de fracasos para hacer el informe
			}

		} catch (Exception e) {
			e.printStackTrace();
			// Pasamos. Solo es una migración

		} finally {
			//4// AHORA CERRAMOS LA CONEXIÓN A FICHERO, no solo se cierra la BBDD.
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

		//5//HEMOS MIGRADO. AHORA CREAMOS UN FICHERO DE INFORME junto al fichero
		//de donde hemos cogido los datos a migrar (PATH_FICHERO)
		
		
		File fichero = new File(PATH_FICHERO_RES);
		if ( fichero.exists() ){
			fichero.delete();
		} 

		FileWriter fw = null;
		PrintWriter pw = null;		
		
		try{		
			fw = new FileWriter(PATH_FICHERO_RES);
			pw = new PrintWriter(fw, true); 
			
			pw.println("-----------------MIGRACIÓN OK-----------------");
			pw.println("Se han importado "+contador_si+ "registros correctos");
			pw.println("Se han importado "+contador_no+ "registros incorrectos");
			
			
		}catch(Exception e){
			
			e.printStackTrace();
			
		}finally {
			
			try {
				if (pw!=null) { pw.close(); }			
				if (fw!=null) { fw.close(); }
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			
		}		

		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	}//FIN del main
	
}//EOF
