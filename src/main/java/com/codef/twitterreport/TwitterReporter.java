package com.codef.twitterreport;

import java.awt.Desktop;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import org.apache.log4j.Logger;

import twitter4j.IDs;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;

public class TwitterReporter {

	private static final Logger LOGGER = Logger.getLogger(TwitterReporter.class.getName());

	public static final String TWITTER_ID = "TWITTER_ID";
	public static final String TWREFRESHDATA = "TWREFRESHDATA";
	public static final String TWNEVERREPORTNF = "TWNEVERREPORTNF";
	public static final String TWNEVERREPORTFL = "TWNEVERREPORTFL";
	public static final String TWDEFAULTIMAGE = "TWDEFAULTIMAGE";
	public static final String TWIMAGEURL = "TWIMAGEURL";
	public static final String TWNAME = "TWNAME";
	public static final String TWSCREENNAME = "TWSCREENNAME";
	public static final String TWLOCATION = "TWLOCATION";
	public static final String TWDESCRIPTION = "TWDESCRIPTION";
	public static final String TWFRIENDSCOUNT = "TWFRIENDSCOUNT";
	public static final String TWFOLLOWERSCOUNT = "TWFOLLOWERSCOUNT";
	public static final String ROWCOUNT = "ROWCOUNT";

	public static final String TABLE_START = "<table cellpadding=\"3\" cellspacing=\"3\" style=\"width: 90%\">";
	public static final String TABLE_END = "</table>";

	public static final Twitter twitterInstance = TwitterFactory.getSingleton();

	private static Map<String, String> metricsMap = new HashMap<>();

	private static Set<Long> peopleWhoUnfollowed = new HashSet<>();
	private static Set<Long> peopleWhoFollowed = new HashSet<>();

	private static Set<Long> peopleWhoFollowMeThatIDontFollow = new HashSet<>();
	private static Set<Long> peopleWhoDontFollowMeThatIDoFollow = new HashSet<>();

	private static int totalUserGets = 1;
	private static String fullPathToTwitterReport = "";

	public static void main(String[] args) {

		try {

			Scanner scanner = new Scanner(System.in);

			LOGGER.info("Drop tables? (YES, or ENTER for NO)");
			String rebuildYes = scanner.next();

			Properties properties = TwitterReporter.loadProperties("TwitterReporter.properties");
			if (properties != null) {
				fullPathToTwitterReport = properties.getProperty("fullPathToTwitterReport");
			}

			Path templatePath = Paths.get(ClassLoader.getSystemResource("templates/mainpage.html").toURI());
			String mainpage = new String(Files.readAllBytes(templatePath), StandardCharsets.UTF_8);
			templatePath = Paths.get(ClassLoader.getSystemResource("templates/recordrow.html").toURI());
			String recordrow = new String(Files.readAllBytes(templatePath), StandardCharsets.UTF_8);

			Connection h2Conn = getH2Connection();
			if (rebuildYes.equals("YES")) {
				dropTables(h2Conn);
				createTable(h2Conn);
			}

			doIntialLoadAndMetrics(h2Conn);

			mainpage = mainpage.replace("STATSMAP", new StringBuilder(metricsMap.toString()));
			mainpage = mainpage.replace("DIV1_1_VALUE", metricsMap.get("TOTAL_NEW_FOLLOWS"));
			mainpage = mainpage.replace("DIV1_2_VALUE", metricsMap.get("TOTAL_UNFOLLOWS"));
			mainpage = mainpage.replace("DIV2_1_VALUE", metricsMap.get("TOTAL_PEOPLE_YOU_DONT_FOLLOW"));
			mainpage = mainpage.replace("DIV2_2_VALUE",
					metricsMap.get("TOTAL_PEOPLE_YOU_FOLLOW_THAT_DOESNT_FOLLOW_YOU"));
			mainpage = mainpage.replace("DIV1_1", getHTMLForTweepsRow(h2Conn, peopleWhoFollowed, recordrow));
			mainpage = mainpage.replace("DIV1_2", getHTMLForTweepsRow(h2Conn, peopleWhoUnfollowed, recordrow));
			mainpage = mainpage.replace("DIV2_1",
					getHTMLForTweepsRow(h2Conn, peopleWhoFollowMeThatIDontFollow, recordrow));
			mainpage = mainpage.replace("DIV2_2",
					getHTMLForTweepsRow(h2Conn, peopleWhoDontFollowMeThatIDoFollow, recordrow));

			writeStringBuilderToFile(mainpage);
			openFileInInternetExplorer();

			closeH2Connection(h2Conn);

			scanner.close();

		} catch (Exception e) {
			LOGGER.error(e.toString(), e);
		}

		System.exit(0);

	}

	public static void dropTables(Connection conn) throws SQLException {

		try (Statement stmt = conn.createStatement();) {
			stmt.executeUpdate("DROP TABLE CURRENT_FOLLOWERS");
			stmt.executeUpdate("DROP TABLE PRIOR_FOLLOWERS");
			stmt.executeUpdate("DROP TABLE FOLLOWERS");
			LOGGER.info("Tables dropped");
		} catch (Exception e) {
			LOGGER.error(e.toString(), e);
		}
	}

	public static void createTable(Connection conn) throws SQLException {

		try (Statement stmt = conn.createStatement();) {
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS CURRENT_FOLLOWERS (TWITTER_ID BIGINT PRIMARY KEY)");
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS PRIOR_FOLLOWERS (TWITTER_ID BIGINT PRIMARY KEY)");
			stmt.executeUpdate(
					"CREATE TABLE IF NOT EXISTS FOLLOWERS (TWITTER_ID BIGINT PRIMARY KEY, TWREFRESHDATA TINYINT, "
							+ "TWNEVERREPORTNF TINYINT, TWNEVERREPORTFL TINYINT, TWNAME VARCHAR(50), "
							+ "TWSCREENNAME VARCHAR(50), TWLOCATION VARCHAR(150), TWDESCRIPTION VARCHAR(200), TWDEFAULTIMAGE VARCHAR(5), "
							+ "TWIMAGEURL VARCHAR(300), TWFRIENDSCOUNT INT, TWFOLLOWERSCOUNT INT  )");
			LOGGER.info("Tables made");
		} catch (Exception e) {
			LOGGER.error(e.toString(), e);
		}
	}

	public static Connection getH2Connection() throws SQLException {
		return DriverManager.getConnection("jdbc:h2:~/twitterreporter", "sa", "");
	}

	public static void closeH2Connection(Connection conn) throws SQLException {
		conn.close();
	}

	public static void doIntialLoadAndMetrics(Connection conn) throws TwitterException {

		Set<Long> peopleYouFollow = getPeopleYouFollowIds();
		metricsMap.put("TOTAL_PEOPLE_YOU_FOLLOW", Integer.toString(peopleYouFollow.size()));

		copyCurrentFollowersToPreviousFollowersTable(conn);

		Set<Long> currentPeopleWhoFollowYou = getCurrentPeopleWhoFollowYouIds();
		metricsMap.put("TOTAL_CURRENT_FOLLOWERS", Integer.toString(currentPeopleWhoFollowYou.size()));

		addToCurrentFollowersTable(conn, currentPeopleWhoFollowYou);

		Set<Long> previousPeopleWhoFollowYou = getPreviousPeopleWhoFollowYouIds(conn);
		metricsMap.put("TOTAL_PREVIOUS_FOLLOWERS", Integer.toString(previousPeopleWhoFollowYou.size()));

		Collection<Long> priorFollowersCollection = previousPeopleWhoFollowYou;
		Collection<Long> currentFollowersCollection = currentPeopleWhoFollowYou;
		Collection<Long> peopleYouFollowCollection = peopleYouFollow;

		peopleWhoUnfollowed = getPeopleWhoUnfollowedIds(priorFollowersCollection, currentFollowersCollection);
		metricsMap.put("TOTAL_UNFOLLOWS", Integer.toString(peopleWhoUnfollowed.size()));

		peopleWhoFollowed = getPeopleWhoFollowedIds(priorFollowersCollection, currentFollowersCollection);
		metricsMap.put("TOTAL_NEW_FOLLOWS", Integer.toString(peopleWhoFollowed.size()));

		peopleWhoFollowMeThatIDontFollow = getPeopleWhoFollowMeThatIDontFollow(peopleYouFollowCollection,
				currentFollowersCollection);
		metricsMap.put("TOTAL_PEOPLE_YOU_DONT_FOLLOW", Integer.toString(peopleWhoFollowMeThatIDontFollow.size()));

		peopleWhoDontFollowMeThatIDoFollow = getPeopleWhoDontFollowMeThatIDoFollow(peopleYouFollowCollection,
				currentFollowersCollection);
		metricsMap.put("TOTAL_PEOPLE_YOU_FOLLOW_THAT_DOESNT_FOLLOW_YOU",
				Integer.toString(peopleWhoDontFollowMeThatIDoFollow.size()));

	}

	public static void addToCurrentFollowersTable(Connection conn, Set<Long> currentPeopleWhoFollowYou) {
		for (Long twitterId : currentPeopleWhoFollowYou) {
			String insertSql = "INSERT INTO CURRENT_FOLLOWERS VALUES (" + twitterId + ")";
			try (Statement stmt = conn.createStatement();) {
				stmt.executeUpdate(insertSql);
			} catch (Exception e) {
				LOGGER.error(e.toString(), e);
			}
		}
	}

	public static void copyCurrentFollowersToPreviousFollowersTable(Connection conn) {

		try (Statement stmt = conn.createStatement();) {
			stmt.executeUpdate("DELETE FROM PRIOR_FOLLOWERS");
		} catch (Exception e) {
			LOGGER.error(e.toString(), e);
		}

		try (Statement stmt = conn.createStatement();
				Statement stmt2 = conn.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT TWITTER_ID FROM CURRENT_FOLLOWERS");) {
			while (rs.next()) {
				stmt2.executeUpdate("INSERT INTO PRIOR_FOLLOWERS VALUES (" + rs.getLong(TWITTER_ID) + ")");
			}
		} catch (SQLException e) {
			LOGGER.error(e.toString(), e);
		}

		try (Statement stmt = conn.createStatement();) {
			stmt.executeUpdate("DELETE FROM CURRENT_FOLLOWERS");
		} catch (Exception e) {
			LOGGER.error(e.toString(), e);
		}

	}

	public static Properties loadProperties(String resourceName) {
		ClassLoader oloader = ClassLoader.getSystemClassLoader();
		try (InputStream oInputStream = oloader.getResource(resourceName).openStream()) {
			Properties oProps = new Properties();
			oProps.load(oInputStream);
			return oProps;
		} catch (IOException e) {
			LOGGER.error(e.toString(), e);
			return null;
		}
	}

	public static Set<Long> getPeopleWhoFollowMeThatIDontFollow(Collection<Long> peopleYouFollowCollection,
			Collection<Long> currentFollowersCollection) {
		Collection<Long> differenceCollection = new HashSet<>(currentFollowersCollection);
		differenceCollection.removeAll(peopleYouFollowCollection);
		return new HashSet<>(differenceCollection);
	}

	public static Set<Long> getPeopleWhoDontFollowMeThatIDoFollow(Collection<Long> peopleYouFollowCollection,
			Collection<Long> currentFollowersCollection) {
		Collection<Long> differenceCollection = new HashSet<>(peopleYouFollowCollection);
		differenceCollection.removeAll(currentFollowersCollection);
		return new HashSet<>(differenceCollection);
	}

	public static Set<Long> getPeopleWhoUnfollowedIds(Collection<Long> priorFollowersCollection,
			Collection<Long> currentFollowersCollection) {
		Collection<Long> differenceCollection = new HashSet<>(priorFollowersCollection);
		differenceCollection.removeAll(currentFollowersCollection);
		return new HashSet<>(differenceCollection);
	}

	public static Set<Long> getPeopleWhoFollowedIds(Collection<Long> priorFollowersCollection,
			Collection<Long> currentFollowersCollection) {
		Collection<Long> differenceCollection = new HashSet<>(currentFollowersCollection);
		differenceCollection.removeAll(priorFollowersCollection);
		return new HashSet<>(differenceCollection);
	}

	public static Set<Long> getPeopleYouFollowIds() throws TwitterException {

		IDs friendIds = twitterInstance.getFriendsIDs(-1);
		long[] friendIdArray = friendIds.getIDs();
		HashSet<Long> peopleYouFollowHashSet = new HashSet<>();
		for (int i = 0; i < friendIdArray.length; i++) {
			peopleYouFollowHashSet.add(friendIdArray[i]);
		}
		return peopleYouFollowHashSet;
	}

	public static Set<Long> getCurrentPeopleWhoFollowYouIds() throws TwitterException {

		IDs friendIds = twitterInstance.getFollowersIDs(-1);
		long[] friendIdArray = friendIds.getIDs();
		HashSet<Long> peopleWhoFollowYouHashSet = new HashSet<>();
		for (int i = 0; i < friendIdArray.length; i++) {
			peopleWhoFollowYouHashSet.add(friendIdArray[i]);
		}
		return peopleWhoFollowYouHashSet;
	}

	public static Set<Long> getPreviousPeopleWhoFollowYouIds(Connection conn) {

		Set<Long> returnSet = new HashSet<>();

		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT TWITTER_ID FROM PRIOR_FOLLOWERS");) {
			while (rs.next()) {
				returnSet.add(rs.getLong(TWITTER_ID));
			}
		} catch (SQLException e) {
			LOGGER.error(e.toString(), e);
		}

		return returnSet;

	}

	public static void writeStringBuilderToFile(String content) throws IOException {
		Files.write(Paths.get(fullPathToTwitterReport), content.getBytes());
	}

	/*
	 * START PRESENTATION CODE
	 */

	public static void openFileInInternetExplorer() {
		try {
			Desktop desktop = java.awt.Desktop.getDesktop();
			URI oURL = new URI(fullPathToTwitterReport.replaceAll("\\\\", "/"));
			desktop.browse(oURL);
		} catch (Exception e) {
			LOGGER.error(e.toString(), e);
		}
	}

	public static StringBuilder getHTMLForTweepsRow(Connection conn, Collection<Long> peopleCollection,
			String recordrow) throws InterruptedException {

		int rowCount = 1;

		StringBuilder sb = new StringBuilder();
		sb.append(TABLE_START);

		for (Long singleUserId : peopleCollection) {

			String newrow = recordrow;

			Map<String, String> singleUser = getUser(conn, singleUserId);

			if (singleUser != null) {

				newrow = newrow.replace(ROWCOUNT, Integer.toString(rowCount));
				newrow = newrow.replace(TWITTER_ID, getEmptyValueIfNull(singleUser.get(TWITTER_ID)));
				newrow = newrow.replace(TWREFRESHDATA, getEmptyValueIfNull(singleUser.get(TWREFRESHDATA)));
				newrow = newrow.replace(TWNEVERREPORTNF, getEmptyValueIfNull(singleUser.get(TWNEVERREPORTNF)));
				newrow = newrow.replace(TWNEVERREPORTFL, getEmptyValueIfNull(singleUser.get(TWNEVERREPORTFL)));
				newrow = newrow.replace(TWDEFAULTIMAGE, getEmptyValueIfNull(singleUser.get(TWDEFAULTIMAGE)));
				newrow = newrow.replace(TWIMAGEURL, getEmptyValueIfNull(singleUser.get(TWIMAGEURL)));
				newrow = newrow.replace(TWNAME, getEmptyValueIfNull(singleUser.get(TWNAME)));
				newrow = newrow.replace(TWSCREENNAME, getEmptyValueIfNull(singleUser.get(TWSCREENNAME)));
				newrow = newrow.replace(TWLOCATION, getEmptyValueIfNull(singleUser.get(TWLOCATION)));
				newrow = newrow.replace(TWDESCRIPTION, getEmptyValueIfNull(singleUser.get(TWDESCRIPTION)));
				newrow = newrow.replace(TWFRIENDSCOUNT, getEmptyValueIfNull(singleUser.get(TWFRIENDSCOUNT)));
				newrow = newrow.replace(TWFOLLOWERSCOUNT, getEmptyValueIfNull(singleUser.get(TWFOLLOWERSCOUNT)));
				sb.append(newrow);
				rowCount++;
			}

		}

		sb.append(TABLE_END);
		return sb;

	}

	public static StringBuilder getEmptyValueIfNull(String value) {
		if (value == null) {
			return new StringBuilder("");
		} else {
			return new StringBuilder(value);
		}
	}

	public static Map<String, String> getUserFromDb(Connection conn, Long singleUserId) {

		Map<String, String> userMap = new HashMap<>();

		try (PreparedStatement pStmt = conn.prepareStatement("SELECT * FROM FOLLOWERS WHERE TWITTER_ID = ?")) {

			pStmt.setLong(1, singleUserId);

			try (ResultSet rs = pStmt.executeQuery();) {
				while (rs.next()) {

					LOGGER.info("Getting user " + singleUserId + " from database.");

					userMap.put(TWITTER_ID, rs.getString(TWITTER_ID));
					userMap.put(TWREFRESHDATA, rs.getString(TWREFRESHDATA));
					userMap.put(TWNEVERREPORTNF, rs.getString(TWNEVERREPORTNF));
					userMap.put(TWNEVERREPORTFL, rs.getString(TWNEVERREPORTFL));
					userMap.put(TWDEFAULTIMAGE, rs.getString(TWDEFAULTIMAGE));
					userMap.put(TWIMAGEURL, rs.getString(TWIMAGEURL));
					userMap.put(TWNAME, rs.getString(TWNAME));
					userMap.put(TWSCREENNAME, rs.getString(TWSCREENNAME));
					userMap.put(TWLOCATION, rs.getString(TWLOCATION));
					userMap.put(TWDESCRIPTION, rs.getString(TWDESCRIPTION));
					userMap.put(TWFRIENDSCOUNT, rs.getString(TWFRIENDSCOUNT));
					userMap.put(TWFOLLOWERSCOUNT, rs.getString(TWFOLLOWERSCOUNT));
				}
			}

		} catch (SQLException e) {
			LOGGER.error(e.toString(), e);
		}

		return userMap;

	}

	public static Map<String, String> putUserInDb(Connection conn, User singleUser, Long singleUserId) {

		Map<String, String> userMap = new HashMap<>();

		try (PreparedStatement pStmt = conn
				.prepareStatement("insert into FOLLOWERS values (?,?,?,?,?,?,?,?,?,?,?,?)");) {

			pStmt.setLong(1, singleUserId);
			pStmt.setInt(2, 0);
			pStmt.setInt(3, 0);
			pStmt.setInt(4, 0);
			pStmt.setString(5, singleUser.getName());
			pStmt.setString(6, singleUser.getScreenName());
			pStmt.setString(7, singleUser.getLocation());
			pStmt.setString(8, singleUser.getDescription());
			pStmt.setString(9, Boolean.toString(singleUser.isDefaultProfileImage()));
			pStmt.setString(10, singleUser.getOriginalProfileImageURL());
			pStmt.setInt(11, singleUser.getFriendsCount());
			pStmt.setInt(12, singleUser.getFollowersCount());
			pStmt.executeUpdate();

		} catch (SQLException e) {
			LOGGER.error(e.toString(), e);
		}

		return userMap;

	}

	public static Map<String, String> getUser(Connection conn, Long singleUserId) throws InterruptedException {

		boolean profileNotFound = false;

		Map<String, String> singleUser = getUserFromDb(conn, singleUserId);

		if (singleUser.isEmpty()) {

			ResponseList<User> peoples = null;
			boolean success = false;
			while (!success && !profileNotFound) {
				try {

					LOGGER.info("Getting user " + singleUserId + " from Twitter.");
					peoples = getUserList(singleUserId);
					success = true;

				} catch (Exception e) {

					if (e.toString().startsWith("404:")) {
						profileNotFound = true;
					} else {
						int retryInSeconds = getRetryInSeconds(e);
						showRetryWarning(retryInSeconds);
						Thread.sleep(retryInSeconds * 1000L);
					}
				}
			}

			if (profileNotFound) {
				return null;
			} else {
				totalUserGets = totalUserGets + 1;
				putUserInDb(conn, peoples.get(0), singleUserId);
				return getUserFromDb(conn, singleUserId);
			}

		} else {
			return singleUser;
		}

	}

	public static ResponseList<User> getUserList(Long userId) throws TwitterException {
		long[] userIdArray = new long[1];
		userIdArray[0] = userId;
		return twitterInstance.lookupUsers(userIdArray);
	}

	public static void showRetryWarning(int retryInSeconds) {

		long retryDate = System.currentTimeMillis();
		Timestamp original = new Timestamp(retryDate);
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(original.getTime());
		cal.add(Calendar.MILLISECOND, (retryInSeconds * 1000));
		Timestamp later = new Timestamp(cal.getTime().getTime());
		LOGGER.warn("Twitter throttled, retry at " + new SimpleDateFormat("h:mm:ss a").format(later));

	}

	public static int getRetryInSeconds(Exception e) {

		String secondsUntilResetString = "";
		String[] stringExceptions = e.toString().split("=");
		for (int j = 0; j < stringExceptions.length; j++) {
			if (stringExceptions[j].endsWith("secondsUntilReset")) {
				secondsUntilResetString = stringExceptions[j + 1].substring(0, stringExceptions[j + 1].indexOf('}'));
			}
		}

		int retryInSeconds = Integer.parseInt(secondsUntilResetString);
		if (retryInSeconds < 0) {
			retryInSeconds = retryInSeconds * -1;
		}
		return retryInSeconds + 15;
	}

	/*
	 * END PRESENTATION CODE
	 */

}
