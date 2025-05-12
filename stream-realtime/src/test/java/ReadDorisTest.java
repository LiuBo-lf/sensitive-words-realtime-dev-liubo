import java.sql.*;

/**
 * @ Package PACKAGE_NAME.ReadDorisTest
 * @ Author  liu.bo
 * @ Date  2025/5/12 15:49
 * @ description:
 * @ version 1.0
 */
public class ReadDorisTest {
    // JDBC 连接参数
    private static final String JDBC_URL = "jdbc:mysql://192.168.200.142:9030/dev_t_zh";
    private static final String USER = "root";
    private static final String PASSWORD = "root";
    // 查询语句，用于获取测试数据
    private static final String QUERY_SQL = "SELECT * FROM dev_t_zh.dws_trade_cart_add_uu_window LIMIT 10";

    /**
     * 程序入口点
     * 使用JDBC连接到Doris数据库，执行查询并打印结果
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        // 定义数据库连接，声明，和结果集变量
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            // 加载并注册JDBC驱动
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 建立数据库连接
            conn = DriverManager.getConnection(
                    JDBC_URL + "?serverTimezone=Asia/Shanghai&useSSL=false",
                    USER,
                    PASSWORD
            );

            // 创建声明对象，用于执行SQL查询
            stmt = conn.createStatement();
            // 执行查询并获取结果集
            rs = stmt.executeQuery(QUERY_SQL);
            // 获取结果集元数据
            ResultSetMetaData metaData = rs.getMetaData();
            // 获取列数
            int columnCount = metaData.getColumnCount();
            // 遍历并打印列名
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(metaData.getColumnName(i) + "\t");
            }
            System.out.println();

            // 遍历结果集并打印每行数据
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (ClassNotFoundException | SQLException e) {
            // 处理可能的异常
            e.printStackTrace();
        } finally {
            // 关闭资源以避免内存泄漏
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

