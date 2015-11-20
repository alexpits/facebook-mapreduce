package social;

import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.social.facebook.api.Facebook;
import org.springframework.social.facebook.api.Post;
import org.springframework.social.facebook.api.User;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping("/")
public class FacebookController extends BaseController {

	@Autowired
	private Facebook facebook;

	@RequestMapping(method = RequestMethod.POST)
	public String downloadFacebook(HttpSession session,
			@RequestParam(value = "userId", required = false) String userId) {
		if (userId == null || userId.isEmpty()) {
			userId = "me";
		}
		session.setAttribute("facebookProfile", facebook.fetchObject(userId, User.class, "id", "name"));
		List<Post> homeFeed = facebook.feedOperations().getFeed(userId);
		homeFeed = homeFeed.stream().filter(p -> p.getMessage() != null && !p.getMessage().isEmpty())
				.collect(Collectors.toList());
		session.setAttribute("feed", homeFeed);
		return "content";
	}

}