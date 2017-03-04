package com.cs499.AS3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class findTopMoviesAndUsers 
{
	private String path1;
	private String path2;
	private String path3;
	private String writePath;
	private String writePath2;
	
	private HashMap<Integer, String> moviesList = new HashMap<Integer, String>();
	
	findTopMoviesAndUsers(String path1, String path2, String path3, String path4, String path5)
	{
		this.path1 = path1;
		this.path2 = path2;
		this.path3 = path3;
		
		this.writePath = path4;
		this.writePath2 = path5;
		new File("FinalResults").mkdir();
	}
	public void storemoviesList() throws FileNotFoundException, IOException
	{
		BufferedReader read = new BufferedReader(new FileReader(path1));
		String currentMovie;
		
		while((currentMovie = read.readLine()) != null)
		{
			String currentMovieAttributes[] = currentMovie.split(",", 3);
			moviesList.put(Integer.parseInt(currentMovieAttributes[0]), currentMovieAttributes[2]);
		}
		
		read.close();
	}
	public void findTop10Movies() throws FileNotFoundException, IOException
	{	
		BufferedReader read = new BufferedReader(new FileReader(path2 + "/part-r-00000"));
		BufferedWriter write = new BufferedWriter(new FileWriter(writePath));
		String currentTopMovie;
		int movieNumber = 1;
		
		System.out.println("Top Ten Movies:\n");
		write.write("Top Ten Movies:\n");
		write.newLine();
		
		while(((currentTopMovie = read.readLine()) != null && movieNumber != 11))
		{
			String currentTopMovieAttributes[] = currentTopMovie.split("\t");
			System.out.println(movieNumber + " " + moviesList.get(Integer.parseInt(currentTopMovieAttributes[1])));
			write.write(movieNumber + " " + moviesList.get(Integer.parseInt(currentTopMovieAttributes[1])));
			write.newLine();
			movieNumber++;
		}
		
		System.out.println();
		read.close();
		write.close();
	}
	public void findTopTenUsers() throws FileNotFoundException, IOException
	{	
		BufferedReader read = new BufferedReader(new FileReader(path3 + "/part-r-00000"));
		BufferedWriter write = new BufferedWriter(new FileWriter(writePath2));
		String currentTopUser;
		int userNumber = 1;
		
		System.out.println("Top Ten Users:\n");
		write.write("Top Ten Users:\n");
		write.newLine();
		
		while((currentTopUser = read.readLine()) != null && userNumber != 11 )
		{
			String currentTopUserAttributes[] = currentTopUser.split("\t");
			System.out.println(userNumber + " " + currentTopUserAttributes[1]);
			write.write(userNumber + " " + currentTopUserAttributes[1]);
			write.newLine();
			userNumber++;
		}
		
		read.close();
		write.close();
	}
}