package social;

import javax.servlet.http.HttpSession;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/hbase")
public class HBaseController {

	@RequestMapping(method = RequestMethod.GET)
	public String loginFacebook() {
		return "redirect:/connect/facebook";
	}

	@RequestMapping(method = RequestMethod.POST)
	public String uploadFacebook(HttpSession session) {
		return "done";
	}

}