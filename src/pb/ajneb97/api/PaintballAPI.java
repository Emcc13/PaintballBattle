package pb.ajneb97.api;

import java.util.ArrayList;

import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;

import pb.ajneb97.PaintballBattle;
import pb.ajneb97.database.JugadorDatos;
import pb.ajneb97.database.MySQL;
import pb.ajneb97.juego.EstadoPartida;
import pb.ajneb97.juego.JugadorPaintballDatos;
import pb.ajneb97.juego.Partida;

public class PaintballAPI {

	private static PaintballBattle plugin;
	
	public PaintballAPI(PaintballBattle plugin) {
		this.plugin = plugin;
	}
	
	public static int getCoins(Player player) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				return j.getCoins();
			}else {
				return 0;
			}
		}else {
			return MySQL.getCoins(plugin, player.getUniqueId().toString());
		}
	}
	
	public static void addCoins(Player player,int coins) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				j.aumentarCoins(coins);
			}
		}else {
			MySQL.updatePlayerCoinsAsync(plugin, player.getUniqueId().toString(), coins);
		}
	}
	
	public static void removeCoins(Player player,int coins) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				j.disminuirCoins(coins);
			}
		}else {
			MySQL.updatePlayerCoinsAsync(plugin, player.getUniqueId().toString(), -coins);
		}
	}
	
	public static int getWins(Player player) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				return j.getWins();
			}else {
				return 0;
			}
		}else {
			return MySQL.getWins(plugin, player.getUniqueId().toString());
		}
		
	}
	
	public static int getLoses(Player player) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				return j.getLoses();
			}else {
				return 0;
			}
		}else {
			return MySQL.getLose(plugin, player.getUniqueId().toString());
		}
		
	}
	
	public static int getTies(Player player) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				return j.getTies();
			}else {
				return 0;
			}
		}else {
			return MySQL.getTie(plugin, player.getUniqueId().toString());
		}
	}
	
	public static int getKills(Player player) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				return j.getKills();
			}else {
				return 0;
			}
		}else {
			return MySQL.getKills(plugin, player.getUniqueId().toString());
		}
		
	}
	
	public static int getPerkLevel(Player player,String perk) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				return j.getNivelPerk(perk);
			}else {
				return 0;
			}
		}else {
			return MySQL.getNivelPerk(plugin, player.getName(), perk);
		}
	}
	
	public static boolean hasHat(Player player,String hat) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				return j.tieneHat(hat);
			}else {
				return false;
			}
		}else {
			return MySQL.jugadorTieneHat(plugin, player.getName(), hat);
		}
	}
	
	public static boolean hasHatSelected(Player player,String hat) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				return j.tieneHatSeleccionado(hat);
			}else {
				return false;
			}
		}else {
			return MySQL.jugadorTieneHatSeleccionado(plugin, player.getName(), hat);
		}
	}
	
	public static ArrayList<Perk> getPerks(Player player) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null) {
				return j.getPerks();
			}else {
				return new ArrayList<Perk>();
			}
		}else {
			return MySQL.getPerksJugador(plugin, player.getName());
		}
	}
	
	public static ArrayList<Hat> getHats(Player player) {
		if(!MySQL.isEnabled(plugin.getConfig())) {
			JugadorDatos j = plugin.getJugador(player.getName());
			if(j != null && j.getHats() != null) {
				return j.getHats();
			}else {
				return new ArrayList<Hat>();
			}
		}else {
			return MySQL.getHatsJugador(plugin, player.getName());
		}
	}
	
	public static int getPlayersArena(String arena) {
		Partida partida = plugin.getPartida(arena);
		if(partida != null) {
			return partida.getCantidadActualJugadores();
		}else {
			return 0;
		}
	}
	
	public static String getStatusArena(String arena) {
		Partida partida = plugin.getPartida(arena);
		FileConfiguration messages = plugin.getMessages();
		if(partida != null) {
			if(partida.getEstado().equals(EstadoPartida.COMENZANDO)) {
				return messages.getString("signStatusStarting");
			}else if(partida.getEstado().equals(EstadoPartida.ESPERANDO)) {
				return messages.getString("signStatusWaiting");
			}else if(partida.getEstado().equals(EstadoPartida.JUGANDO)) {
				return messages.getString("signStatusIngame");
			}else if(partida.getEstado().equals(EstadoPartida.TERMINANDO)) {
				return messages.getString("signStatusFinishing");
			}else {
				return messages.getString("signStatusDisabled");
			}
		}else {
			return null;
		}
	}
}
