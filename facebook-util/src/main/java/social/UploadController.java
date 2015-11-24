package social;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.social.facebook.api.Post;
import org.springframework.social.facebook.api.User;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/upload")
public class UploadController extends AbstractController {

	@Autowired
	private HBaseService hBase;

	@RequestMapping(method = RequestMethod.POST)
	public String upload(HttpSession session) throws IOException {
		User facebookProfile = (User) session.getAttribute("facebookProfile");
		@SuppressWarnings("unchecked")
		List<Post> feed = (List<Post>) session.getAttribute("feed");
		hBase.upload(facebookProfile, feed);
		return "done";
	}

}