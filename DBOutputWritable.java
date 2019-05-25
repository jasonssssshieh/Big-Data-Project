import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable{
	private String starting_phrase;
	private String following_word;
	private int count;
	
	public DBOutputWritable(String starting_phrase, String following_word, int count){
		this.starting_phrase = starting_phrase;
		this.following_word = following_word;
		this.count = count;
	}

	public void readFiles(ResultSet arg0) throws SQLException{
		
	}

	public word write(PreparedStatement agr0) throws SQLException{

	}
}