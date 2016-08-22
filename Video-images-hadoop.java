package com.test;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JPanel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import com.Dao.HadoopDAO;
import com.vs.constants.Constants;
import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.MediaListenerAdapter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.mediatool.event.IVideoPictureEvent;
import com.xuggle.xuggler.Global;

public class Vedio_splitting2 
{
	private JPanel outerjpanel, distributepanel;
	private JButton distribute_btn;

	// //

	//static String Clients[] = { "client", "neighbour1", "neighbour2" };
	static int counter = 0;
	static String fname = "";
	static int i = 1;
	private static int mVideoStreamIndex = -1;
    static StringBuffer sb=new StringBuffer();
	// Time of last frame write
	private static long mLastPtsWrite = Global.NO_PTS;

	public static final double SECONDS_BETWEEN_FRAMES = 0.10;

	public static final long MICRO_SECONDS_BETWEEN_FRAMES = (long) (Global.DEFAULT_PTS_PER_SECOND * SECONDS_BETWEEN_FRAMES);

	private static final String outputFilePrefix = "C:/SampleVedioFiles/";

	public static void main(String[] args) {
		VideoSpliting("atrium.avi");
	}

	private static void VideoSpliting(String string) 
	{
		String fname = Constants.fileName;
		
		boolean flag=HadoopDAO.insertFilename(fname);
		
		if (fname != "") {
		
			IMediaReader mediaReader = ToolFactory
					.makeReader("C:\\SampleVedioFiles\\" + fname);

			// stipulate that we want BufferedImages created
			// in BGR 24bit color space
			mediaReader
					.setBufferedImageTypeToGenerate(BufferedImage.TYPE_3BYTE_BGR);

			mediaReader
					.addListener(new ImageSnapListener());

			// read out the contents of the media file and
			// dispatch events to the attached listener
			while (mediaReader.readPacket() == null)
				;

			
			
		}
		
	}
	private static class ImageSnapListener extends MediaListenerAdapter {

		public void onVideoPicture(IVideoPictureEvent event) {

			if (event.getStreamIndex() != mVideoStreamIndex) {
				// if the selected video stream id is not yet set, go ahead an
				// select this lucky video stream
				if (mVideoStreamIndex == -1)
					mVideoStreamIndex = event.getStreamIndex();
				// no need to show frames from this video stream
				else
					return;
			}

			// if uninitialized, back date mLastPtsWrite to get the very first
			// frame
			if (mLastPtsWrite == Global.NO_PTS)
				mLastPtsWrite = event.getTimeStamp()
						- MICRO_SECONDS_BETWEEN_FRAMES;

			// if it's time to write the next frame
			if (event.getTimeStamp() - mLastPtsWrite >= MICRO_SECONDS_BETWEEN_FRAMES) {

				String outputFilename = dumpImageToFile(event.getImage());
				
				try {
					
					
					
					
					 String filePath = "C:\\New_Workspace\\VedioSplitting\\config.properties";
					System.out.println("filepath upload is >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+outputFilename);
					FileInputStream fileInputStream = new FileInputStream(new File(
							filePath));
					Properties properties = new Properties();
					properties.load(fileInputStream);

					Configuration conf = new Configuration();
					conf.addResource(new Path(properties.getProperty("hadoopLoc")));
					FileSystem fs = FileSystem.get(conf);
					Path pt = new Path(properties.getProperty("hdfsLoc"));

					Path src = new Path(outputFilename);
					fs.copyFromLocalFile(src, pt);
					//fs.co
					
					
				} catch (Exception e) {
					e.printStackTrace();
				}

				
				
				
			
				//-----inserting to the database--starts--------
				File file = new File(outputFilename);
				System.out.println(file.getName());
                 int fileid=HadoopDAO.getFileid();
				
				int id=HadoopDAO.insertframes(file.getName(), fileid,outputFilename);
				sb.append(id);
				sb.append("-");
				
				boolean falg=HadoopDAO.updateLFA(sb.toString(),Constants.fileName);
				//-----inserting to the database---ends-------

				
				
				
				
				
				// indicate file written
				double seconds = ((double) event.getTimeStamp())
						/ Global.DEFAULT_PTS_PER_SECOND;
				 //System.out.printf("at elapsed time of %6.3f seconds wrote: %s\n",seconds, outputFilename);

				// update last write time
				mLastPtsWrite += MICRO_SECONDS_BETWEEN_FRAMES;
			}

		}

		private String dumpImageToFile(BufferedImage image) 
		{
			try {

				Image background_img = image.getScaledInstance(350, 200,
						java.awt.Image.SCALE_SMOOTH);
				ImageIcon imageIcon = new ImageIcon(background_img);
				 int fileid=HadoopDAO.getFileid();
				String outputFilename = outputFilePrefix +fileid+"_"+ i + ".png";
				
				
				ImageIO.write(image, "png", new File(outputFilename));
				
				
				

				i++;

				return outputFilename;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

	}

}

