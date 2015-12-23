package com.ipartek.formacion.migracion.conexion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Clase que permite conectar con la base de datos
 * 
 * @author chenao
 *
 */
public class conexion {
	/** Parametros de conexion */
	static String bd = "iparsex";
	static String login = "root";
	static String password = "";
	static String url = "jdbc:mysql://localhost/" + bd;

	Connection connection = null;

	/** Constructor de DbConnection */
	public conexion() {
		try {

			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(url, login, password);
			
		} catch (Exception e) {
			e.printStackTrace();
			//Típicamente se anidan tipos de excepciones, sql p.ej, pero para
			//este proposito no nos interesa ni discernir, ni trabajarlas. 
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public void desconectar() {
		connection = null;
	}
}