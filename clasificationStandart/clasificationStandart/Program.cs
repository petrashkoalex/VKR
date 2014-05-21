using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace clasificationStandart
{
    class Program
    {
        static void Main(string[] args)
        {
            string Connect = "Database=rs2;Data Source=127.0.0.1;User Id=root;Password=1234";
            MySqlConnection myConnection = new MySqlConnection(Connect);

            string queryStringClearTable = @"truncate table ratingsduplicate2;
                                             truncate table userfilmssummfilms;
                                             truncate table userfilmsperesech;
                                             truncate table userfilmsobiedineniehelp;
                                             truncate table distances1;
                                             truncate table distances2;
                                             truncate table distances2recomend;
                                             truncate table clasificationclaster;
                                             truncate table ksosed;
                                             truncate table recomendation;
                                             delete from ratingsduplicate where userid=1;
                                             delete from userfilmskolvoclaster1 where userid=1;
                                             delete from userfilmskolvo where userid=1;";
            MySqlCommand myCommandClearTable = new MySqlCommand(queryStringClearTable, myConnection);



            myConnection.Open();
            myCommandClearTable.ExecuteNonQuery();

            int ClassificationUserId = 2;
            Console.Write("Write pls userId");
            Console.Write("\n");
            ClassificationUserId = int.Parse(Console.ReadLine());
            Console.Write("\n");
            DateTime time1 = DateTime.Now;
            Console.WriteLine(time1);

            //копируем оценки нового пользователя в таблицу для анализа
            string queryStringZapolnaemTableRatingsduplicate2 = @"insert into ratingsduplicate2 select * from ratings where userid=" + ClassificationUserId;
            MySqlCommand myCommandZapolnaemTableRatingsduplicate2 = new MySqlCommand(queryStringZapolnaemTableRatingsduplicate2, myConnection);
            myCommandZapolnaemTableRatingsduplicate2.CommandTimeout = 0;
            myCommandZapolnaemTableRatingsduplicate2.ExecuteNonQuery();
            //копируем оценки всех польхзователей для анализа
            string queryStringED = @"insert into ratingsduplicate2 select * from ratingsforclaster";
            MySqlCommand myCommandED = new MySqlCommand(queryStringED, myConnection);
            myCommandED.CommandTimeout = 0;
            myCommandED.ExecuteNonQuery();


            //определяем расстояние между всевозможными парами пользователей (заносим информацию в таблицу distances2)
            string queryStringEuclideanDistance = @"insert into distances2 
                                                            select t.user1Id, t.user2Id, (1/(1+(sqrt(sum(t.raznost2))))) as distance2
                                                            from (select a1.userId as User1Id, a2.userId as User2Id, (a1.rating - a2.rating)*(a1.rating - a2.rating) as raznost2
                                                                  from ratingsduplicate2 as a1 inner join ratingsduplicate2 as a2 on a1.filmId=a2.filmId and a1.userid= " + ClassificationUserId + @" and a1.userId<a2.userId)as t
                                                            group by t.user1Id, t.user2Id";
            MySqlCommand myCommandEuclideanDistance = new MySqlCommand(queryStringEuclideanDistance, myConnection);
            myCommandEuclideanDistance.CommandTimeout = 0;
            myCommandEuclideanDistance.ExecuteNonQuery();

            //заполняем таблицу distances2recomend
            string queryStringdistances2recomend = @"insert into distances2recomend select * from distances2 order by distance2 desc limit 10";
            MySqlCommand myCommanddistances2recomend = new MySqlCommand(queryStringdistances2recomend, myConnection);
            myCommanddistances2recomend.CommandTimeout = 0;
            myCommanddistances2recomend.ExecuteNonQuery();

            //прогнозирование оценки пользователя по еще не просмотренным фильмам + выдача рекомендации
            string queryStringPrognozOcenki = @"insert into recomendation select user1Id,filmid, sumR/sumP as rating 
                                                from (select *, sum(R) as sumR, sum(distance2) as sumP 
                                                      from (select *,a1.distance2*a2.rating as R 
                                                            from (select * 
                                                                  from distances2recomend) as a1 inner join 
							 				                      (select * from ratingsforclaster where filmid not in (select filmid from ratings where userid=" + ClassificationUserId + @")) as a2
                                                                  on a1.user2id=a2.userid) as d
							 				          group by filmid) as f";
            MySqlCommand myCommandPrognozOcenki = new MySqlCommand(queryStringPrognozOcenki, myConnection);
            myCommandPrognozOcenki.CommandTimeout = 0;
            myCommandPrognozOcenki.ExecuteNonQuery();


            Console.WriteLine("Recomendation:");
            Console.Write("\n");
            DateTime time2 = DateTime.Now;
            Console.WriteLine(time2);

            string queryStringCountRec = @"select count(*) from recomendation";
            MySqlCommand myCommandCountRec = new MySqlCommand(queryStringCountRec, myConnection);
            myCommandCountRec.CommandTimeout = 0;
            string stringCountRec = myCommandCountRec.ExecuteScalar().ToString();
            int CountRec = Convert.ToInt32(stringCountRec);

            string[] RecFilmName = new string[CountRec];
            string[] RecRating = new string[CountRec];


            string queryStringRec = @"select a2.`Название`,a1.rating from recomendation as a1 inner join films as a2 on a1.filmid=a2.id order by rating desc";
            MySqlCommand myCommandRec = new MySqlCommand(queryStringRec, myConnection);
            myCommandRec.CommandTimeout = 0;
            MySqlDataReader RecWrite = myCommandRec.ExecuteReader();
            int z = 0;
            while (RecWrite.Read())
            {
                RecFilmName[z] = RecWrite.GetString(0);
                RecRating[z] = RecWrite.GetString(1);
                Console.WriteLine(RecFilmName[z]);
                Console.WriteLine(RecRating[z]);
                Console.Write("\n");
                z++;
            }
            RecWrite.Close();
            Console.ReadLine();


            myConnection.Close();



        }
    }
}
