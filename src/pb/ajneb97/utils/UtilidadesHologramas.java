package pb.ajneb97.utils;

import java.util.ArrayList;

import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.configuration.file.FileConfiguration;

import pb.ajneb97.PaintballBattle;
import pb.ajneb97.database.JugadorDatos;
import pb.ajneb97.database.MySQL;
import pb.ajneb97.database.MySQLCallback;

public class UtilidadesHologramas {

	public static int getCantidadLineasHolograma(PaintballBattle plugin) {
		FileConfiguration config = plugin.getConfig();
		FileConfiguration messages = plugin.getMessages();
		int lineas = messages.getStringList("topHologramFormat").size();
		lineas = lineas+Integer.valueOf(config.getString("top_hologram_number_of_players"));
		return lineas;
	}
	
	public static double determinarY(Location location, int cantidadLineasHolograma) {
		double cantidad = cantidadLineasHolograma*0.15;
		return cantidad;
	}
	
	//Este metodo se usa solo para monthly o weekly
		public static void getTopPlayersSQL(final PaintballBattle plugin,final String tipo,final String periodo,final MySQLCallback callback){	
			Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable() {
	            @Override
	            public void run() {
	        		ArrayList<JugadorDatos> jugadores;
	        		if(periodo.equals("monthly")) {
	        			jugadores = MySQL.getPlayerDataMonthly(plugin);
	        		}else if(periodo.equals("weekly")) {
	        			jugadores = MySQL.getPlayerDataWeekly(plugin);
	        		}else {
	        			jugadores = MySQL.getPlayerData(plugin);
	        		}
					if (tipo.equals("kills"))
						jugadores.sort((p1, p2)->{
							if (p1.getKills()>p2.getKills())
								return -1;
							else if (p1.getKills()==p2.getKills())
								return 0;
							else
								return 1;
						});
					else if (tipo.equals("wins"))
						jugadores.sort((p1, p2)->{
							if (p1.getWins()>p2.getWins())
								return -1;
							else if (p1.getWins()==p2.getWins())
								return 0;
							else
								return 1;
						});
	        		Bukkit.getScheduler().runTask(plugin, new Runnable() {
	                    @Override
	                    public void run() {
	                        // call the callback with the result
	                        callback.alTerminar(jugadores);
	                    }
	                });
	            }
	        });
			
		}
		
		public static void getTopPlayers(final PaintballBattle plugin,final ArrayList<JugadorDatos> jugadores,final String tipo,final MySQLCallback callback){
			Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable() {
	            @Override
	            public void run() {
	            	final ArrayList<JugadorDatos> playersList;
	            	if(!MySQL.isEnabled(plugin.getConfig())) {
						playersList = jugadores;
	            	}else {
						playersList = MySQL.getPlayerData(plugin);
	            	}
					if (tipo.equals("kills"))
						jugadores.sort((p1, p2)->{
							if (p1.getKills()>p2.getKills())
								return -1;
							else if (p1.getKills()==p2.getKills())
								return 0;
							else
								return 1;
						});
					else if (tipo.equals("wins"))
						jugadores.sort((p1, p2)->{
							if (p1.getWins()>p2.getWins())
								return -1;
							else if (p1.getWins()==p2.getWins())
								return 0;
							else
								return 1;
						});
	            	Bukkit.getScheduler().runTask(plugin, new Runnable() {
	                    @Override
	                    public void run() {
	                        // call the callback with the result
	                        callback.alTerminar(playersList);
	                    }
	                });
	            }
			});
			
		}
}
