import java.util.*;
import org.wtf.client.Client;
public class Basic {
	public static void main (String[] args) {
		System.out.println("start");
		Client c = new Client(args[0], Integer.valueOf(args[1]), args[2], Integer.valueOf(args[3]));
		int[] status = new int[1];
		status[0] = 0;
		//long fd = c.open("/file1", O_CREAT | O_RDWR, 0777, 3, 4096, status);
		long fd = c.open("/file1", 0x42, 0777, 3, 4096, status);
        assert(fd > 0);
		System.out.println("open fd " + fd + " status " + status[0]);

		byte[] data = "hi".getBytes();
		long[] data_sz = { 2L };
        long reqid2 = -1;
		long reqid = c.write(fd, data, data_sz, 3, status);
        assert(reqid > 0);
        reqid2 = c.loop(reqid, -1, status);
		assert(reqid2 == reqid) : "reqid2: " + reqid2 + " != reqid " + reqid;

		c.close(fd, status);
		System.out.println("close fd " + fd + " status " + status[0]);

        fd = c.open("/file1", 0x2, 0777, 3, 4096, status);
        assert(fd > 0) : c.error_location() + ":" + c.error_message();
		System.out.println("open fd " + fd + " status " + status[0]);

        byte[] readdata = "xx".getBytes(); 
        reqid = -1;
        reqid2 = -1;
        reqid = c.read(fd, readdata, data_sz, status);
        assert(reqid > 0);
        reqid2 = c.loop(reqid, -1, status);
		assert(reqid2 == reqid) : "reqid2: " + reqid2 + " != reqid " + reqid + " status: " + status[0] + " " + c.error_message() + " " + c.error_location();
        assert(Arrays.equals(readdata, data));
        System.out.println(new String(readdata));
		System.out.println("finish");
	}
}
