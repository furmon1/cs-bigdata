package cs.bigdata.ex27;

public class Tree {
	public int year;
	public float height;
	
	public static void displayYearHeightFromLine(String line){
		// Splitting the line to parse it year is [5], height [6]
		String[] infos = line.split(";");
		
		// Filtering trees with no year or no height
		if (!infos[5].equals("") && !infos[6].equals(""))
		{
			// Print the wanted output
			System.out.println(String.format("Year: %s   Height: %s", infos[5], infos[6]));	
		}
	}
}
