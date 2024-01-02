package pb.ajneb97.database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Date;
import java.util.*;
import java.util.logging.Level;

import org.bukkit.Bukkit;
import org.bukkit.configuration.file.FileConfiguration;

import pb.ajneb97.PaintballBattle;
import pb.ajneb97.api.Hat;
import pb.ajneb97.api.Perk;

public class MySQL {

    public static boolean isEnabled(FileConfiguration config) {
        if (config.getString("mysql-database.enabled").equals("true")) {
            return true;
        } else {
            return false;
        }
    }

//    For each match an entry is created with the date, the player uuid, if lost, tie or won, the arena name, kills and coins
    public static void createTablePlayerMatchStats(ConexionDatabase connection){
        try {
            PreparedStatement statement = connection.getConnection().prepareStatement(
                "CREATE TABLE IF NOT EXISTS " +
                connection.getTablePlayers() +
                " (" +
                "MATCHID BIGINT NOT NULL, "+
                "Date DATE NOT NULL," +
                "UUID CHAR(36) NOT NULL," +
                "Name varchar(40) NOT NULL, "+
                "MATCHRESULT INT(1) NOT NULL, "+
                "ARENA VARCHAR(40), " +
                "KILLS INT, " +
                "PRIMARY KEY (MATCHID, Date, UUID)" +
                ");"
            );
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createTableCoins(ConexionDatabase connection){
        try{
            PreparedStatement statement = connection.getConnection().prepareStatement(
                    "CREATE TABLE IF NOT EXISTS " +
                            connection.getTablePlayerCoins() +
                            " (" +
                            "UUID CHAR(36) PRIMARY KEY," +
                            "Coins INT" +
                            ");"
            );
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createTablePerks(ConexionDatabase conexion) {
        try {
            PreparedStatement statement = conexion.getConnection().prepareStatement("CREATE TABLE IF NOT EXISTS " + conexion.getTablePerks() + " (`UUID` varchar(200), `Name` varchar(40), `Perk` varchar(40), `Level` INT(2) )");
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createTableHats(ConexionDatabase conexion) {
        try {
            PreparedStatement statement = conexion.getConnection().prepareStatement("CREATE TABLE IF NOT EXISTS " + conexion.getTableHats() + " (`UUID` varchar(200), `Name` varchar(40), `Hat` varchar(40), `Selected` INT(2) )");
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static int getWins(PaintballBattle plugin, String uuid){
        try{
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement(
                    "SELECT SUM(CASE MATCHRESULT WHEN 1 THEN 1 ELSE 0 END) as Win FROM "+
                            plugin.getConexionDatabase().getTablePlayers()+
                            " WHERE (UUID=?);"
            );
            statement.setString(1, uuid);
            ResultSet rs = statement.executeQuery();
            while (rs.next()){
                return rs.getInt(1);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return -1;
    }

    public static int getTie(PaintballBattle plugin, String uuid){
        try{
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement(
                    "SELECT SUM(CASE MATCHRESULT WHEN 0 THEN 1 ELSE 0 END) as Tie FROM "+
                            plugin.getConexionDatabase().getTablePlayers()+
                            " WHERE (UUID=?);"
            );
            statement.setString(1, uuid);
            ResultSet rs = statement.executeQuery();
            while (rs.next()){
                return rs.getInt(1);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return -1;
    }

    public static int getLose(PaintballBattle plugin, String uuid){
        try{
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement(
                    "SELECT SUM(CASE MATCHRESULT WHEN -1 THEN 1 ELSE 0 END) as Lose FROM "+
                            plugin.getConexionDatabase().getTablePlayers()+
                            " WHERE (UUID=?);"
            );
            statement.setString(1, uuid);
            ResultSet rs = statement.executeQuery();
            while (rs.next()){
                return rs.getInt(1);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return -1;
    }

    public static int getKills(PaintballBattle plugin, String uuid){
        try{
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement(
                    "SELECT SUM(KILLS) as Kills FROM "+
                            plugin.getConexionDatabase().getTablePlayers()+
                            " WHERE (UUID=?);"
            );
            statement.setString(1, uuid);
            ResultSet rs = statement.executeQuery();
            while (rs.next()){
                return rs.getInt(1);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return -1;
    }

    public static int getCoins(PaintballBattle plugin, String uuid){
        try{
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement(
                    "SELECT Coins FROM "+
                            plugin.getConexionDatabase().getTablePlayerCoins()+
                            " WHERE (UUID=?);"
            );
            statement.setString(1, uuid);
            ResultSet rs = statement.executeQuery();
            while (rs.next()){
                return rs.getInt(1);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return -1;
    }

    public static void insertMatchPlayerDataAsync(final PaintballBattle plugin, final Long matchid, final Date date,
                                                  final String uuid, final String player, final int matchresult,
                                                  final String arena, final int kills){
        Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable() {
            @Override
            public void run() {
                try {
                    PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement(
                            "INSERT INTO "+
                                    plugin.getConexionDatabase().getTablePlayers() +
                                    " (MATCHID, Date, UUID, Name, MATCHRESULT, ARENA, KILLS) VALUES (?, ?, ?, ?, ?, ?, ?);"
                    );
                    statement.setLong(1, matchid);
                    statement.setDate(2, date);
                    statement.setString(3, uuid);
                    statement.setString(4, player);
                    statement.setInt(5, matchresult);
                    statement.setString(6, arena);
                    statement.setInt(7, kills);
                    statement.executeUpdate();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    public static void updatePlayerCoinsAsync(final PaintballBattle plugin, final String uuid, final int coins) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable() {
            @Override
            public void run() {
                try {
                    PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement(
                            "INSERT INTO "+
                                    plugin.getConexionDatabase().getTablePlayerCoins()+
                                    " (UUID, Coins) VALUES (?, ?) ON DUPLICATE KEY UPDATE UUID=?, Coins=Coins+?;"
                    );
//                    PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement("UPDATE " + plugin.getConexionDatabase().getTablePlayers() + " SET Coins=`Coins`+? WHERE (Name=? AND Global_Data=1)");
                    statement.setString(1, uuid);
                    statement.setInt(2, coins);
                    statement.setString(3, uuid);
                    statement.setInt(4, coins);
                    statement.executeUpdate();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    public static boolean jugadorTieneHat(PaintballBattle plugin, String player, String hat) {
        try {
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement("SELECT * FROM " + plugin.getConexionDatabase().getTableHats() + " WHERE (Name=? AND Hat=?)");
            statement.setString(1, player);
            statement.setString(2, hat);
            ResultSet resultado = statement.executeQuery();
            if (resultado.next()) {
                return true;
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    public static void agregarJugadorHatAsync(final PaintballBattle plugin, final String uuid, final String name, final String hat) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable() {
            @Override
            public void run() {
                try {
                    PreparedStatement insert = plugin.getConexionDatabase().getConnection()
                            .prepareStatement("INSERT INTO " + plugin.getConexionDatabase().getTableHats() + " (UUID,Name,Hat,Selected) VALUE (?,?,?,?)");
                    insert.setString(1, uuid);
                    insert.setString(2, name);
                    insert.setString(3, hat);
                    insert.setInt(4, 0);
                    insert.executeUpdate();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    public static boolean jugadorTieneHatSeleccionado(PaintballBattle plugin, String player, String hat) {
        try {
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement("SELECT * FROM " + plugin.getConexionDatabase().getTableHats() + " WHERE (Name=? AND Hat=? AND Selected=1)");
            statement.setString(1, player);
            statement.setString(2, hat);
            ResultSet resultado = statement.executeQuery();
            if (resultado.next()) {
                return true;
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    public static ArrayList<Hat> getHatsJugador(PaintballBattle plugin, String name) {
        ArrayList<Hat> hats = new ArrayList<Hat>();
        try {
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement("SELECT * FROM " + plugin.getConexionDatabase().getTableHats() + " WHERE (Name=?)");
            statement.setString(1, name);
            ResultSet resultado = statement.executeQuery();
            while (resultado.next()) {
                String hat = resultado.getString("Hat");
                int selected = resultado.getInt("Selected");
                boolean selectedB = false;
                if (selected == 1) {
                    selectedB = true;
                }
                hats.add(new Hat(hat, selectedB));
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return hats;
    }

    public static void deseleccionarHats(final PaintballBattle plugin, final String player) {
        try {
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement("UPDATE " + plugin.getConexionDatabase().getTableHats() + " SET Selected=0 WHERE (Name=? AND Selected=1)");
            statement.setString(1, player);
            statement.executeUpdate();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void seleccionarHatAsync(final PaintballBattle plugin, final String player, final String hat) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable() {
            @Override
            public void run() {
                try {
                    deseleccionarHats(plugin, player);

                    PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement("UPDATE " + plugin.getConexionDatabase().getTableHats() + " SET Selected=1 WHERE (Name=? AND Hat=?)");
                    statement.setString(1, player);
                    statement.setString(2, hat);
                    statement.executeUpdate();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    public static void crearJugadorPerkAsync(final PaintballBattle plugin, final String uuid, final String name, final String perk) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable() {
            @Override
            public void run() {
                try {
                    PreparedStatement insert = plugin.getConexionDatabase().getConnection()
                            .prepareStatement("INSERT INTO " + plugin.getConexionDatabase().getTablePerks() + " (UUID,Name,Perk,Level) VALUE (?,?,?,?)");
                    insert.setString(1, uuid);
                    insert.setString(2, name);
                    insert.setString(3, perk);
                    insert.setInt(4, 1);
                    insert.executeUpdate();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });

    }

    public static int getNivelPerk(PaintballBattle plugin, String name, String perk) {
        int level = 0;
        try {
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement("SELECT * FROM " + plugin.getConexionDatabase().getTablePerks() + " WHERE (Name=? AND Perk=?)");
            statement.setString(1, name);
            statement.setString(2, perk);
            ResultSet resultado = statement.executeQuery();
            while (resultado.next()) {
                level = resultado.getInt("Level");
            }

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return level;
    }

    public static boolean jugadorPerkExiste(PaintballBattle plugin, String player, String perk) {
        try {
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement("SELECT * FROM " + plugin.getConexionDatabase().getTablePerks() + " WHERE (Name=? AND Perk=?)");
            statement.setString(1, player);
            statement.setString(2, perk);
            ResultSet resultado = statement.executeQuery();
            if (resultado.next()) {
                return true;
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    public static ArrayList<Perk> getPerksJugador(PaintballBattle plugin, String name) {
        ArrayList<Perk> perks = new ArrayList<Perk>();
        try {
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement("SELECT * FROM " + plugin.getConexionDatabase().getTablePerks() + " WHERE (Name=?)");
            statement.setString(1, name);
            ResultSet resultado = statement.executeQuery();
            while (resultado.next()) {
                String perk = resultado.getString("Perk");
                int level = resultado.getInt("Level");
                perks.add(new Perk(perk, level));
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return perks;
    }

    public static void setPerkJugadorAsync(final PaintballBattle plugin, final String uuid, final String player, final String perk, final int level) {
        if (jugadorPerkExiste(plugin, player, perk)) {
            Bukkit.getScheduler().runTaskAsynchronously(plugin, new Runnable() {
                @Override
                public void run() {
                    try {
                        PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement("UPDATE " + plugin.getConexionDatabase().getTablePerks() + " SET Level=? WHERE (Name=? AND Perk=?)");
                        statement.setInt(1, level);
                        statement.setString(2, player);
                        statement.setString(3, perk);
                        statement.executeUpdate();
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            });
        } else {
            crearJugadorPerkAsync(plugin, uuid, player, perk);
        }

    }

    public static ArrayList<JugadorDatos> getPlayerDataMonthly(PaintballBattle plugin) {

        Map<String, JugadorDatos> players = new HashMap();
        try {
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement(
                    "SELECT UUID as UUID, " +
                            "Name as Name, " +
                            "SUM(CASE MATCHRESULT WHEN 1 THEN 1 ELSE 0 END) as Win, " +
                            "SUM(CASE MATCHRESULT WHEN 0 THEN 1 ELSE 0 END) as Tie, " +
                            "SUM(CASE MATCHRESULT WHEN -1 THEN 1 ELSE 0 END) as Lose, " +
                            "SUM(KILLS) as Kills FROM " +
                            plugin.getConexionDatabase().getTablePlayers() +
                            " WHERE (YEAR(Date)=YEAR(CURDATE()) AND MONTH(Date)=MONTH(CURDATE())) GROUP BY (UUID);"
            );
            ResultSet resultado = statement.executeQuery();
            while (resultado.next()) {
                String name = resultado.getString("Name");
                String uuid = resultado.getString("UUID");
                if (!players.containsKey(uuid)){
                    int wins = resultado.getInt("Win");
                    int loses = resultado.getInt("Lose");
                    int ties = resultado.getInt("Tie");
                    int kills = resultado.getInt("Kills");
                    JugadorDatos p = new JugadorDatos(name, uuid, wins, loses, ties, kills, 0, null, null);
                    players.put(uuid, p);
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return new ArrayList<>(players.values());
    }

    public static ArrayList<JugadorDatos> getPlayerDataWeekly(PaintballBattle plugin) {
        Map<String, JugadorDatos> players = new HashMap<>();
        try {
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement(
                    "SELECT UUID as UUID, " +
                            "Name as Name, " +
                            "SUM(CASE MATCHRESULT WHEN 1 THEN 1 ELSE 0 END) as Win, " +
                            "SUM(CASE MATCHRESULT WHEN 0 THEN 1 ELSE 0 END) as Tie, " +
                            "SUM(CASE MATCHRESULT WHEN -1 THEN 1 ELSE 0 END) as Lose, " +
                            "SUM(KILLS) as Kills FROM " +
                            plugin.getConexionDatabase().getTablePlayers() +
                            " WHERE (YEAR(Date)=YEAR(CURDATE()) AND WEEK(Date)=WEEK(CURDATE())) GROUP BY (UUID);"
            );
            ResultSet resultado = statement.executeQuery();
            while (resultado.next()) {
                String name = resultado.getString("Name");
                String uuid = resultado.getString("UUID");
                if (!players.containsKey(uuid)){
                    int wins = resultado.getInt("Win");
                    int loses = resultado.getInt("Lose");
                    int ties = resultado.getInt("Tie");
                    int kills = resultado.getInt("Kills");
                    JugadorDatos p = new JugadorDatos(name, uuid, wins, loses, ties, kills, 0, null, null);
                    players.put(uuid, p);
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return new ArrayList<>(players.values());
    }

    //Se cargan solo las globales
    public static ArrayList<JugadorDatos> getPlayerData(PaintballBattle plugin) {
        Map<String, JugadorDatos> players = new HashMap<>();
        try {
            PreparedStatement statement = plugin.getConexionDatabase().getConnection().prepareStatement(
                    "SELECT UUID as UUID, " +
                            "Name as Name," +
                            "SUM(CASE MATCHRESULT WHEN 1 THEN 1 ELSE 0 END) as Win, " +
                            "SUM(CASE MATCHRESULT WHEN 0 THEN 1 ELSE 0 END) as Tie, " +
                            "SUM(CASE MATCHRESULT WHEN -1 THEN 1 ELSE 0 END) as Lose, " +
                            "SUM(KILLS) as Kills " +
                            "FROM " +
                            plugin.getConexionDatabase().getTablePlayers() +
                            " GROUP BY (UUID);"
            );
            ResultSet resultado = statement.executeQuery();
            while (resultado.next()) {
                String name = resultado.getString("Name");
                String uuid = resultado.getString("UUID");
                if (!players.containsKey(uuid)){
                    int wins = resultado.getInt("Win");
                    int loses = resultado.getInt("Lose");
                    int ties = resultado.getInt("Tie");
                    int kills = resultado.getInt("Kills");
                    JugadorDatos p = new JugadorDatos(name, uuid, wins, loses, ties, kills, 0, null, null);
                    players.put(uuid, p);
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return new ArrayList<>(players.values());
    }
}
