package social;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

public class BaseController {
	
	@RequestMapping(method = RequestMethod.GET)
	public String loginFacebook() {
		return "redirect:/connect/facebook";
	}

}
