import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;
import java.util.Properties;
import org.apache.pinot.client.PinotDriver;
import io.github.cdimascio.dotenv.Dotenv;

public class PinotQuery {
    public static void main(String[] args) throws Throwable {
        // Load environment variables from .env file
        Dotenv dotenv = Dotenv.load();

        String dbUrl = dotenv.get("DB_URL");
        String username = dotenv.get("USERNAME");
        String password = dotenv.get("PASSWORD");

        // Concatenate username and password and use base64 to encode the concatenated string
        String plainCredentials = username + ":" + password;
        String base64Credentials = new String(Base64.getEncoder().encode(plainCredentials.getBytes()));

        // Create authorization header
        String authorizationHeader = "Basic " + base64Credentials;
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("headers.Authorization", authorizationHeader);

        // Register new Pinot JDBC driver
        DriverManager.registerDriver(new PinotDriver());

        // Get a client connection and set the encoded authorization header
        Connection connection = DriverManager.getConnection(dbUrl, connectionProperties);

        // Test that your query successfully authenticates
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM baseballStats LIMIT 1;");

        while (rs.next()) {
            String result = rs.getString("count(*)");
            System.out.println(result);
        }
    }
}
