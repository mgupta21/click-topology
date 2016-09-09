package storm.cookbook;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CountryStats {

    private int              countryTotal     = 0;

    private static final int COUNT_INDEX      = 0;
    private static final int PERCENTAGE_INDEX = 1;
    private String           countryName;

    public CountryStats(String countryName) {
        this.countryName = countryName;
    }

    private Map<String, List<Integer>> cityStats = new HashMap<String, List<Integer>>();

    public void cityFound(String cityName) {
        countryTotal++;
        if (cityStats.containsKey(cityName)) {
            cityStats.get(cityName).set(COUNT_INDEX,
                cityStats.get(cityName).get(COUNT_INDEX).intValue() + 1);
        } else {
            List<Integer> list = new LinkedList<Integer>();
            list.add(1);
            list.add(0);
            cityStats.put(cityName, list);
        }

        double percent = (double) cityStats.get(cityName).get(COUNT_INDEX)
            / (double) countryTotal;
        cityStats.get(cityName).set(PERCENTAGE_INDEX, (int) percent);
    }

    public int getCountryTotal() {
        return countryTotal;
    }

    public int getCityTotal(String cityName) {
        return cityStats.get(cityName).get(COUNT_INDEX).intValue();
    }

    public String toString() {
        return "Total Count for " + countryName + " is "
            + Integer.toString(countryTotal) + "\n" + "Cities:  "
            + cityStats.toString();
    }
}
