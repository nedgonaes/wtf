import java.util.*;
import org.wtf.client.Client;
import org.wtf.client.Deferred;
public class Basic {
	public static void main (String[] args) throws Exception {
		System.out.println("start");
		Client c = new Client(args[0], Integer.valueOf(args[1]), args[2], Integer.valueOf(args[3]));
        long[] fd = new long[1];
		fd[0] = 0;
        int offset = 0;

		//long fd = c.open("/file1", O_CREAT | O_RDWR, 0777, 3, 4096);
		Boolean ok = c.open("/file1", 0x42, 0777, 3, 4096, fd);

		System.out.println("open fd " + fd[0]);

		//byte[] data = "hi".getBytes();
        byte[] data = new byte[2];
        data[0] = 'h';
        data[1] = 'i';

		ok = c.write(fd[0], data, offset);
        if (data[0] != 'h')
        {
           return;
        }

		c.close(fd[0]);
		System.out.println("close fd " + fd[0]);

        ok = c.open("/file1", 0x2, 0777, 3, 4096, fd);

		System.out.println("open fd " + fd[0]);
        byte[] readdata = new byte[2]; 
        readdata[0] = '\0';
        readdata[1] = '\0';
        ok = c.read(fd[0], readdata, offset);
        if (readdata[0]!='h')
        {
            return;
        }

        System.out.println(new String(readdata));
		System.out.println("finish");
	}
}
