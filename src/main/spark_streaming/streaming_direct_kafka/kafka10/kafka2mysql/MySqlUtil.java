/**
 * FileName: MySqlUtil
 * Author: SiXiang
 * Date: 2019/5/30 19:16
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * Sixiang 修改时间  版本号    描述
 */
package streaming_direct_kafka.kafka10.kafka2mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 功能简述：
 * 〈 DB工具类 〉
 *
 * @author SiXiang
 * @create 2019/5/30
 * @since 1.0.0
 */
public class MySqlUtil {

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/";

    static final String USER = "root";
    static final String PASSWORD = "123456";

    public static Connection getConnection(String db){
        try {
            return DriverManager.getConnection(DB_URL+db, USER, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

}
