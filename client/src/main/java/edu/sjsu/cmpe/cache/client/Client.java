package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

 public class Client {

     private final static SortedMap<Integer, String> ring = new TreeMap<Integer, String>();
     private static HashFunction hashFunction = Hashing.md5();
     private static ArrayList<String> serverList = new ArrayList<String>();
     static char[] characters = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};

     public static void main(String[] args) throws Exception {

         serverList.add("http://localhost:3000");
         serverList.add("http://localhost:3001");
         serverList.add("http://localhost:3002");
     
         for (int n = 0; n < serverList.size(); n++)
         {
             add(serverList.get(n), n);
         }

         for (int x = 0; x < 10; x++)
         {
             int bucket = Hashing.consistentHash(Hashing.md5().hashInt(x),ring.size());
             String server = get(bucket);
             System.out.println("Routed to server: " + server);
             CacheServiceInterface cache = new DistributedCacheService(server);
             cache.put(x + 1, String.valueOf(characters[x]));
             String value = cache.get(x + 1);
             System.out.println("get(" + (x + 1) + ") => " + value);
         }

         System.out.println("Exiting cache client");
     
     }

     public static void add(String server, int i)
     {
         HashCode hc = hashFunction.hashLong(i);
         ring.put(hc.asInt(), server);
     }

     public static void remove(int key)
     {
         int hash = hashFunction.hashLong(key).asInt();
         ring.remove(hash);
     }

     public static String get(Object key)
     {
         if (ring.isEmpty())
         {
             return null;
         }
     
         int hash = hashFunction.hashLong((Integer) key).asInt();
         
         if (!ring.containsKey(hash))
         {
             SortedMap<Integer, String> tailMap = ring.tailMap(hash);
             hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
         }
         
        return ring.get(hash);
     }
 }

