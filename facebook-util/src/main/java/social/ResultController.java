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
	public String getResult(Model model, @RequestParam(value = "id", required = false) String id) throws IOException {
		if (id == null) {
			model.addAttribute("resultList", hBase.scanResultList());
			return "resultList";
		} else {
			model.addAttribute("user", hBase.getResult(id));
			return "result";
		}
	}

	@RequestMapping(method = RequestMethod.POST)
	public String mapReduce(Model model, @RequestParam(value = "id", required = true) String id) throws IOException {
		model.addAttribute("user", hBase.getResult(id));
		return "result";
	}

}