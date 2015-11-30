package social;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping("/result")
public class ResultController {

	@Autowired
	private HBaseService hBase;

	@RequestMapping(method = RequestMethod.GET)
	public String getResult(Model model, @RequestParam(value = "id", required = false) String userId) throws IOException {
		if (userId == null) {
			model.addAttribute("resultList", hBase.getUsers());
			return "resultList";
		} else {
			final int MIN_COUNT = 5;
			model.addAttribute("user", hBase.getUser(userId));
			model.addAttribute("wordcount", hBase.getWords(userId, MIN_COUNT));
			return "result";
		}
	}

}