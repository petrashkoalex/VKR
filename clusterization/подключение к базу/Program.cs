using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace RecSystem
{
    class Program
    {
        static void Main(string[] args)
        {
            string Connect = "Database=kinopoisk;Data Source=127.0.0.1;User Id=root;Password=1234";
            MySqlConnection myConnection = new MySqlConnection(Connect);            
                    
            
            //удаление всех записей из временных таблиц
            string queryStringDeleteTableUserFilmsKolvo = @"TRUNCATE TABLE userfilmskolvo";
            string queryStringDeleteTableUserFilmsKolvoclaster1 = @"TRUNCATE TABLE userfilmskolvoclaster1";
            string queryStringDeleteTableUserFilmsPeresech = @"TRUNCATE TABLE userfilmsPeresech";
            string queryStringDeleteTableUserFilmssummfilms = @"TRUNCATE TABLE userfilmssummfilms";
            string queryStringDeleteTableuserfilmsobiedinenie = @"TRUNCATE TABLE userfilmsobiedinenie";
            string queryStringDeleteTableuserfilmsobiedineniehelp = @"TRUNCATE TABLE userfilmsobiedineniehelp";
            string queryStringDeleteTableDistances1 = @"TRUNCATE TABLE distances1";
            string queryStringDeleteTableDistances2 = @"TRUNCATE TABLE distances2";
            string queryStringDeleteTableRatingsduplicate = @"TRUNCATE TABLE ratingsduplicate";
            string queryStringDeleteTableRatingsduplicate2 = @"TRUNCATE TABLE ratingsduplicate2";
            string queryStringDeleteTableRatingsduplicateSrednee = @"TRUNCATE TABLE ratingsduplicatesrednee";
            string queryStringDeleteTableClasters1 = @"TRUNCATE TABLE clasters1";
            string queryStringDeleteTableClasters2 = @"TRUNCATE TABLE clasters2";
            string queryStringDeleteTableUsersNonClaster = @"TRUNCATE TABLE usersnonclaster";


            
            MySqlCommand myCommandDeleteTableUserFilmsKolvo = new MySqlCommand(queryStringDeleteTableUserFilmsKolvo, myConnection);
            MySqlCommand myCommandDeleteTableUserFilmsKolvoclaster1 = new MySqlCommand(queryStringDeleteTableUserFilmsKolvoclaster1, myConnection);
            MySqlCommand myCommandDeleteTableUserFilmsPeresech = new MySqlCommand(queryStringDeleteTableUserFilmsPeresech, myConnection);
            MySqlCommand myCommandDeleteTableUserFilmssummfilms = new MySqlCommand(queryStringDeleteTableUserFilmssummfilms, myConnection);
            MySqlCommand myCommandDeleteTableuserfilmsobiedinenie = new MySqlCommand(queryStringDeleteTableuserfilmsobiedinenie, myConnection);
            MySqlCommand myCommandDeleteTableuserfilmsobiedineniehelp = new MySqlCommand(queryStringDeleteTableuserfilmsobiedineniehelp, myConnection);
            MySqlCommand myCommandDeleteTableDistances1 = new MySqlCommand(queryStringDeleteTableDistances1, myConnection);
            MySqlCommand myCommandDeleteTableDistances2 = new MySqlCommand(queryStringDeleteTableDistances2, myConnection);
            MySqlCommand myCommandDeleteTableeRatingsduplicate = new MySqlCommand(queryStringDeleteTableRatingsduplicate, myConnection);
            MySqlCommand myCommandDeleteTableeRatingsduplicate2 = new MySqlCommand(queryStringDeleteTableRatingsduplicate2, myConnection);
            MySqlCommand myCommandDeleteTableRatingsduplicateSrednee = new MySqlCommand(queryStringDeleteTableRatingsduplicateSrednee, myConnection);
            MySqlCommand myCommandDeleteTableeClasters1 = new MySqlCommand(queryStringDeleteTableClasters1, myConnection);
            MySqlCommand myCommandDeleteTableeClasters2 = new MySqlCommand(queryStringDeleteTableClasters2, myConnection);
            MySqlCommand myCommandDeleteTableUsersNonClaster = new MySqlCommand(queryStringDeleteTableUsersNonClaster, myConnection);

            
            
            myConnection.Open();
            /*
            myCommandDeleteTableUserFilmsKolvo.ExecuteNonQuery();
            myCommandDeleteTableUserFilmsKolvoclaster1.ExecuteNonQuery();
            myCommandDeleteTableUserFilmsPeresech.ExecuteNonQuery();
            myCommandDeleteTableUserFilmssummfilms.ExecuteNonQuery();
            myCommandDeleteTableuserfilmsobiedinenie.ExecuteNonQuery();
            myCommandDeleteTableuserfilmsobiedineniehelp.ExecuteNonQuery();
            myCommandDeleteTableDistances1.ExecuteNonQuery();
            myCommandDeleteTableDistances2.ExecuteNonQuery();
            myCommandDeleteTableeRatingsduplicate.ExecuteNonQuery();            
            myCommandDeleteTableRatingsduplicateSrednee.ExecuteNonQuery();
            myCommandDeleteTableeClasters1.ExecuteNonQuery();
            myCommandDeleteTableeClasters2.ExecuteNonQuery();                      
            myCommandDeleteTableUsersNonClaster.ExecuteNonQuery();
            

            

            //Создание копии таблицы ratings (копия необходима, т.к. нужно удалять и апдейтить записи)
            string queryStringZapolnaemTableRatingsduplicate = @"insert into ratingsduplicate select * from ratingsforclaster";
            MySqlCommand myCommandZapolnaemTableRatingsduplicate = new MySqlCommand(queryStringZapolnaemTableRatingsduplicate, myConnection);
            myCommandZapolnaemTableRatingsduplicate.CommandTimeout = 0;
            myCommandZapolnaemTableRatingsduplicate.ExecuteNonQuery();

            //определяем Кол-во фильмов у каждого пользователя
            string queryStringKolvoFilms = @"insert into userfilmskolvo
                                                select userId, count(*)
                                                from ratingsduplicate
                                             group by userId";
            MySqlCommand myCommandKolvoFilms = new MySqlCommand(queryStringKolvoFilms, myConnection);
            myCommandKolvoFilms.CommandTimeout = 0;
            myCommandKolvoFilms.ExecuteNonQuery();
            //дублируем таблицу кол-ва фильмов
            string queryStringKolvoFilmsclaster1 = @"insert into userfilmskolvoclaster1 select * from userfilmskolvo";
            MySqlCommand myCommandKolvoFilmsclaster1 = new MySqlCommand(queryStringKolvoFilmsclaster1, myConnection);
            myCommandKolvoFilmsclaster1.CommandTimeout = 0;
            myCommandKolvoFilmsclaster1.ExecuteNonQuery();

            //определяем кол-во пользователей
            string queryStringCountUser = @" select count(*) from userfilmskolvo";
            MySqlCommand myCommandCountUser = new MySqlCommand(queryStringCountUser, myConnection);
            myCommandCountUser.CommandTimeout = 0;
            string stringCountUser = myCommandCountUser.ExecuteScalar().ToString();
            int CountUser = Convert.ToInt32(stringCountUser);

            //создаем массив с id user
            string[] UserId = new string[CountUser];
            string queryStringUsersId = @"select userId
		                                     from userfilmskolvo
		                                     order by UserId";
            MySqlCommand myCommandUsers1Id = new MySqlCommand(queryStringUsersId, myConnection);
            myCommandUsers1Id.CommandTimeout = 0;
            MySqlDataReader User1Id = myCommandUsers1Id.ExecuteReader();
            int z = 0;
            while (User1Id.Read())
            {
                UserId[z] = User1Id.GetString(0);
                z++;
            }
            User1Id.Close();


            Console.Write("tablici gotovi");
            DateTime time1 = DateTime.Now;
            Console.WriteLine(time1);

            //заполняем таблицу userfilmsperesech и userfilmssummfilms
            for (int p = 0; p <= CountUser - 1; p++)
            {
                //определенеие пересечения 
                string queryStringUserFilmsPeresechUserId = @"insert into userfilmsPeresech
                                                                    select t.User1Id, t.User2Id, count(*) as p1
                                                                    from (select a1.userId as User1Id, a2.userId as User2Id, a1.filmId as filmId
                                                                          from ratingsduplicate as a1 inner join ratingsduplicate as a2 on a1.filmId=a2.filmId 
                                                                          where a1.userId=" + UserId[p] + @" and a1.userId < a2.userId) as t
                                                                    group by t.User1Id, t.User2Id";
                MySqlCommand myCommandUserFilmsPeresechUserId = new MySqlCommand(queryStringUserFilmsPeresechUserId, myConnection);
                myCommandUserFilmsPeresechUserId.CommandTimeout = 0;
                myCommandUserFilmsPeresechUserId.ExecuteNonQuery();


                //Кол-во фильмов пары пользователей (рассматриваются всевозможные пары пользователей)
                string queryStringObsieFilms = @"insert into userfilmssummfilms
                                                    select a1.userId as user1Id, a2.userId as user2Id, (a1.kolvo + a2.kolvo) as p2
                                                    from userfilmskolvo as a1, userfilmskolvo as a2 
                                                    where a1.userId=" + UserId[p] + @" and  a1.userId < a2.userId";
                MySqlCommand myCommandObsieFilms = new MySqlCommand(queryStringObsieFilms, myConnection);
                myCommandObsieFilms.CommandTimeout = 0;
                myCommandObsieFilms.ExecuteNonQuery();
                


                //определение объединения фильмов
                string queryStringuserfilmsobiedineniehelp = @"insert into userfilmsobiedineniehelp select a1.user1Id, a1.user2Id, (a2.p2 - a1.p1) as p3
                                                                                            from userfilmsperesech as a1 inner join userfilmssummfilms as a2 
                                                                                            on a1.user1Id=" + UserId[p] + @" and a2.user1Id=" + UserId[p] + @" and a1.user2Id=a2.user2Id";
                MySqlCommand myCommanduserfilmsobiedineniehelp = new MySqlCommand(queryStringuserfilmsobiedineniehelp, myConnection);
                myCommanduserfilmsobiedineniehelp.CommandTimeout = 0;
                myCommanduserfilmsobiedineniehelp.ExecuteNonQuery();

                //определение коэффициентов Танимото
                string queryStringKTanimoto = @"insert into distances1 select a2.user1id, a2.user2id, a1.p1 / a2.p3 
                                                                from userfilmsperesech as a1, userfilmsobiedineniehelp as a2
                                                                where a1.user1id=" + UserId[p] + @" and a2.user1Id=" + UserId[p] + @" and a1.user2Id=a2.user2Id";
                MySqlCommand myCommandKTanimoto = new MySqlCommand(queryStringKTanimoto, myConnection);
                myCommandKTanimoto.CommandTimeout = 0;
                myCommandKTanimoto.ExecuteNonQuery();

                                
                //удаляем лишние записи из distances1                
                string queryStringDeleteKTanimoto = @"delete from distances1 where distance1<0.2";
                MySqlCommand myCommandDeleteKTanimoto = new MySqlCommand(queryStringDeleteKTanimoto, myConnection);
                myCommandDeleteKTanimoto.CommandTimeout = 0;
                myCommandDeleteKTanimoto.ExecuteNonQuery();

                //
                string queryStringuserfilmsobiedinenie = @"insert into userfilmsobiedinenie select a1.user1id,a1.user2id,a1.p3
                                                                                     from userfilmsobiedineniehelp as a1 inner join distances1 as a2 
                                                                                     on a1.user1id=a2.user1Id and a1.user2id=a2.user2Id";
                MySqlCommand myCommanduserfilmsobiedinenie = new MySqlCommand(queryStringuserfilmsobiedinenie, myConnection);
                myCommanduserfilmsobiedinenie.CommandTimeout = 0;
                myCommanduserfilmsobiedinenie.ExecuteNonQuery();

                //очищаем таблицу userfilmsobiedineniehelp
                myCommandDeleteTableuserfilmsobiedineniehelp.ExecuteNonQuery();

                //userfilmssummfilms
                myCommandDeleteTableUserFilmssummfilms.ExecuteNonQuery();
                //userfilmsperesech
                myCommandDeleteTableUserFilmsPeresech.ExecuteNonQuery();



                Console.Write(p);
                DateTime time2 = DateTime.Now;
                Console.WriteLine(Environment.NewLine);
                Console.WriteLine(time2);
            }


            Console.Write("dannie gotovi");
            DateTime time12 = DateTime.Now;
            Console.WriteLine(Environment.NewLine);
            Console.WriteLine(time12);





            


            



            //ПЕРВАЯ КЛАСТЕРИЗАЦИЯ (KTanimoto)




            //определение пользователей с max коэффициентом Танимото из таблицы distances1
            string queryStringMaxKTanimoto = @"select *  
                                               from distances1 as t 
                                               where t.distance1= (select max(distance1) as max 
                                                                   from distances1)";

            MySqlCommand myCommandMaxKTanimoto = new MySqlCommand(queryStringMaxKTanimoto, myConnection);
            myCommandMaxKTanimoto.CommandTimeout = 0;
            MySqlDataReader MaxKTanimoto = myCommandMaxKTanimoto.ExecuteReader();

            MaxKTanimoto.Read();
            string user1IdTanimoto = MaxKTanimoto.GetString(0);
            string user2IdTanimoto = MaxKTanimoto.GetString(1);
            double KTanimoto = MaxKTanimoto.GetDouble(2);           
            MaxKTanimoto.Close();
            int k = 0;            
            while (KTanimoto >= 0.2)
            {    
                //проверка!!! после объединения пользователей в кластер, не должно нарушиться правило: "каждый пользователь принадлежащий кластеру, посмотрел не меньше половины фильмов сещуствующих в кластере"
                //проверяем, что K1 для всех пользователей образовываемого кластера не будет < 0.3  
                //находим множество объединение фильмов(будущее объединение, для кластера)
                string queryStringFilmsobiedinenieNewClaster = @"select p3 from userfilmsobiedinenie 
                                                                where (user1id=" + user1IdTanimoto + @" and user2id=" + user2IdTanimoto + @")";
                MySqlCommand myCommandFilmsobiedinenieNewClaster = new MySqlCommand(queryStringFilmsobiedinenieNewClaster, myConnection);
                myCommandFilmsobiedinenieNewClaster.CommandTimeout = 0;
                string stringFilmsobiedinenieNewClaster = myCommandFilmsobiedinenieNewClaster.ExecuteScalar().ToString();
                int FilmsobiedinenieNewClaster = Convert.ToInt32(stringFilmsobiedinenieNewClaster);

                int ConstFlag = 1;//Конастанта-флаг, показывающая можно ли объединять кластеры

                //проверяем является ли user1IdTanimoto кластером
                string queryStringUser1IsClaster = @"select count(*) from clasters1 where claster1id =" + user1IdTanimoto;
                MySqlCommand myCommandUser1IsClaster = new MySqlCommand(queryStringUser1IsClaster, myConnection);
                myCommandUser1IsClaster.CommandTimeout = 0;
                string stringUser1IsClaster = myCommandUser1IsClaster.ExecuteScalar().ToString();
                int User1IsClaster = Convert.ToInt32(stringUser1IsClaster);


                //Если 1 юзер кластер, то необходимо проверить что при объединении пользователей в кластер, не будет нарушено правило (20% шанс попасть в множество пересечения фильмов)
                if (User1IsClaster > 0)
                    {
                        string[] UsersIdInClaster1 = new string[User1IsClaster];
                        string queryStringUsersIdInClaster1 = @"select userid from clasters1 where claster1id =" + user1IdTanimoto;

                        MySqlCommand myCommandUsersIdInClaster1 = new MySqlCommand(queryStringUsersIdInClaster1, myConnection);
                        myCommandUsersIdInClaster1.CommandTimeout = 0;
                        MySqlDataReader UserIdClaster1 = myCommandUsersIdInClaster1.ExecuteReader();
                        int l = 0;
                        while (UserIdClaster1.Read())
                        {
                            UsersIdInClaster1[l] = UserIdClaster1.GetString(0);
                            l++;
                        }
                        UserIdClaster1.Close();

                        for (int j = 0; j <= User1IsClaster - 1; j++)
                        {
                            string queryStringKTanimotoUser1IdClaster = @"select count(*)/" + FilmsobiedinenieNewClaster + @" from (select * from ratingsforclaster 
                                                            where userid in (select userid 
                                                                             from clasters1 
						                                                     where claster1id=" + user1IdTanimoto + @" and userid!=" + UsersIdInClaster1[j] + @") 
                                                            or userid in(" + user1IdTanimoto + @"," + user2IdTanimoto + @") or userid in (select userid from clasters1 where claster1id=" + user2IdTanimoto + @")
                                                            group by filmid) as a1 inner join (select * from ratingsforclaster where userid=" + UsersIdInClaster1[j] + @") as a2 
				                                                            on a1.filmId=a2.filmId";
                            MySqlCommand myCommandKTanimotoUser1IdClaster = new MySqlCommand(queryStringKTanimotoUser1IdClaster, myConnection);
                            myCommandKTanimotoUser1IdClaster.CommandTimeout = 0;
                            string stringKTanimotoUser1IdClaster = myCommandKTanimotoUser1IdClaster.ExecuteScalar().ToString();
                            double KTanimotoUser1IdClaster = Convert.ToDouble(stringKTanimotoUser1IdClaster);
                            if (KTanimotoUser1IdClaster < 0.2)
                            {
                                ConstFlag = 0;
                                Console.Write("   koeficient user is 1 claster menshe min");
                            }                            
                        }
                    }

                string queryStringKTanimotoUser1Id = @"select count(*)/" + FilmsobiedinenieNewClaster + @" from (select * from ratingsforclaster 
                                                       where userid in (select userid from clasters1 where claster1id=" + user1IdTanimoto + @") 
																       or userid in (select userid from clasters1 where claster1id=" + user2IdTanimoto + @") or userid=" + user2IdTanimoto + @"
                                                       group by filmid) as a1 inner join (select * from ratingsforclaster where userid=" + user1IdTanimoto + @") as a2 
				                                                      on a1.filmId=a2.filmId";
                MySqlCommand myCommandKTanimotoUser1Id = new MySqlCommand(queryStringKTanimotoUser1Id, myConnection);
                myCommandKTanimotoUser1Id.CommandTimeout = 0;
                string stringKTanimotoUser1Id = myCommandKTanimotoUser1Id.ExecuteScalar().ToString();
                double KTanimotoUser1Id = Convert.ToDouble(stringKTanimotoUser1Id);
                if (KTanimotoUser1Id < 0.2)
                {
                    ConstFlag = 0;
                    Console.Write("   koeficient user1 menshe min");
                }


                //проверяем является ли user2IdTanimoto кластером
                string queryStringUser2IsClaster = @"select count(*) from clasters1 where claster1id =" + user2IdTanimoto;
                MySqlCommand myCommandUser2IsClaster = new MySqlCommand(queryStringUser2IsClaster, myConnection);
                myCommandUser2IsClaster.CommandTimeout = 0;
                string stringUser2IsClaster = myCommandUser2IsClaster.ExecuteScalar().ToString();
                int User2IsClaster = Convert.ToInt32(stringUser2IsClaster);

                //Если 2 юзер кластер, то необходимо проверить что при объединении пользователей в кластер, не будет нарушено правило (20% шанс попасть в множество пересечения фильмов)
                if (User2IsClaster > 0)
                    {
                        string[] UsersIdInClaster1 = new string[User2IsClaster];
                        string queryStringUsersIdInClaster1 = @"select userid from clasters1 where claster1id =" + user2IdTanimoto;

                        MySqlCommand myCommandUsersIdInClaster1 = new MySqlCommand(queryStringUsersIdInClaster1, myConnection);
                        myCommandUsersIdInClaster1.CommandTimeout = 0;
                        MySqlDataReader UserIdClaster1 = myCommandUsersIdInClaster1.ExecuteReader();
                        int l = 0;
                        while (UserIdClaster1.Read())
                        {
                            UsersIdInClaster1[l] = UserIdClaster1.GetString(0);
                            l++;
                        }
                        UserIdClaster1.Close();

                        for (int j = 0; j <= User2IsClaster - 1; j++)
                        {
                            string queryStringKTanimotoUser1IdClaster = @"select count(*)/" + FilmsobiedinenieNewClaster + @" from (select * from ratingsforclaster 
                                                            where userid in (select userid 
                                                                             from clasters1 
						                                                     where claster1id=" + user2IdTanimoto + @" and userid!=" + UsersIdInClaster1[j] + @") 
                                                            or userid in(" + user1IdTanimoto + @"," + user2IdTanimoto + @") or userid in (select userid from clasters1 where claster1id=" + user1IdTanimoto + @")
                                                            group by filmid) as a1 inner join (select * from ratingsforclaster where userid=" + UsersIdInClaster1[j] + @") as a2 
				                                                            on a1.filmId=a2.filmId";
                            MySqlCommand myCommandKTanimotoUser1IdClaster = new MySqlCommand(queryStringKTanimotoUser1IdClaster, myConnection);
                            myCommandKTanimotoUser1IdClaster.CommandTimeout = 0;
                            string stringKTanimotoUser1IdClaster = myCommandKTanimotoUser1IdClaster.ExecuteScalar().ToString();
                            double KTanimotoUser1IdClaster = Convert.ToDouble(stringKTanimotoUser1IdClaster);
                            if (KTanimotoUser1IdClaster < 0.2)
                            {
                                ConstFlag = 0;
                                Console.Write("   koeficient user is 2 claster menshe min");
                            }                            
                        }
                    }

                string queryStringKTanimotoUser2Id = @"select count(*)/" + FilmsobiedinenieNewClaster + @" from (select * from ratingsforclaster 
                                                       where userid in (select userid from clasters1 where claster1id=" + user1IdTanimoto + @") 
																       or userid in (select userid from clasters1 where claster1id=" + user2IdTanimoto + @") or userid=" + user1IdTanimoto + @"
                                                       group by filmid) as a1 inner join (select * from ratingsforclaster where userid=" + user2IdTanimoto + @") as a2 
				                                                      on a1.filmId=a2.filmId";
                MySqlCommand myCommandKTanimotoUser2Id = new MySqlCommand(queryStringKTanimotoUser2Id, myConnection);
                myCommandKTanimotoUser2Id.CommandTimeout = 0;
                string stringKTanimotoUser2Id = myCommandKTanimotoUser2Id.ExecuteScalar().ToString();
                double KTanimotoUser2Id = Convert.ToDouble(stringKTanimotoUser2Id);
                if (KTanimotoUser2Id < 0.2)
                {
                    ConstFlag = 0;
                    Console.Write("   koeficient user2 menshe min");
                }
                        





                //объединяем кластеры, если флаг вернет true
                if (ConstFlag == 1)
                {
                    //объединения user1Id и user2Id в кластер       
                    string queryStringUser2IdInClaster = @"Insert into clasters1 values(" + user1IdTanimoto + @"," + user2IdTanimoto + @");";
                    MySqlCommand myCommandUser2IdInClaster = new MySqlCommand(queryStringUser2IdInClaster, myConnection);
                    myCommandUser2IdInClaster.CommandTimeout = 0;
                    myCommandUser2IdInClaster.ExecuteNonQuery();                                 
                    //всех пользователей кластера 2 (user2Id) определяем в 1 кластер (User1Id)
                    string queryStringUsersClaster2InClaster1 = @"update clasters1 set claster1Id=" + user1IdTanimoto + @" where claster1Id=" + user2IdTanimoto;
                    MySqlCommand myCommandUsersClaster2InClaster1 = new MySqlCommand(queryStringUsersClaster2InClaster1, myConnection);
                    myCommandUsersClaster2InClaster1.CommandTimeout = 0;
                    myCommandUsersClaster2InClaster1.ExecuteNonQuery();

                    //объединение фильмов 1-ого и 2-ого кластера.
                    string queryStringObedinenieOcenokClacterov = @"INSERT INTO ratingsduplicate 
                                                                   (select " + user1IdTanimoto + @", filmId, rating from ratingsduplicate where userId=" + user2IdTanimoto + @")
                                                                   ON DUPLICATE key update userId=" + user1IdTanimoto;
                    MySqlCommand myCommandObedinenieOcenokClacterov = new MySqlCommand(queryStringObedinenieOcenokClacterov, myConnection);
                    myCommandObedinenieOcenokClacterov.CommandTimeout = 0;
                    myCommandObedinenieOcenokClacterov.ExecuteNonQuery();
                    string queryStringUdalenieOcenok2Clastera = @"delete from ratingsduplicate where userId=" + user2IdTanimoto;
                    MySqlCommand myCommandUdalenieOcenok2Clastera = new MySqlCommand(queryStringUdalenieOcenok2Clastera, myConnection);
                    myCommandUdalenieOcenok2Clastera.CommandTimeout = 0;
                    myCommandUdalenieOcenok2Clastera.ExecuteNonQuery();


                    //удаляем все упоминания о user1IdTanimoto и user2IdTanimoto из таблиц userfilmskolvo, userfilmssummfilms, userfilmsPeresech, distances1
                    //и добавляем записи кластера user1IdTanimoto, для определения новых коэффициентов Танимото
                    //userfilmskolvo
                        //удаление записей содержащие user1IdTanimoto или user2IdTanimoto
                    string queryStringDeleteUpominaniyaIsUserfilmskolvoclaster1 = @"delete from userfilmskolvoclaster1 
                                                                                       where userId in (" + user1IdTanimoto + @", " + user2IdTanimoto + @")";
                    MySqlCommand myCommandDeleteUpominaniyaIsUserfilmskolvoclaster1 = new MySqlCommand(queryStringDeleteUpominaniyaIsUserfilmskolvoclaster1, myConnection);
                    myCommandDeleteUpominaniyaIsUserfilmskolvoclaster1.CommandTimeout = 0;
                    myCommandDeleteUpominaniyaIsUserfilmskolvoclaster1.ExecuteNonQuery();
                        //добавление записей с user1IdTanimoto
                    string queryStringUserFilmsKolvoUser1IdTanimoto = @"insert into userfilmskolvoclaster1
                                                                    select userId, count(*)
                                                                    from ratingsduplicate
                                                                    where userId=" + user1IdTanimoto;
                    MySqlCommand myCommandUserFilmsKolvoUser1IdTanimoto = new MySqlCommand(queryStringUserFilmsKolvoUser1IdTanimoto, myConnection);
                    myCommandUserFilmsKolvoUser1IdTanimoto.CommandTimeout = 0;
                    myCommandUserFilmsKolvoUser1IdTanimoto.ExecuteNonQuery();


                    //userfilmssummfilms
                    //добавление записей с user1IdTanimoto
                    string queryStringUserFilmssummfilmsUser1IdTanimoto = @"insert into userfilmssummfilms
                                                                        select a1.userId as user1Id, a2.userId as user2Id, (a1.Kolvo + a2.Kolvo) as p2
                                                                        from userfilmskolvoclaster1 as a1 join userfilmskolvoclaster1 as a2
                                                                        where a1.userId=" + user1IdTanimoto + @" and " + user1IdTanimoto + @"<a2.userId";
                    MySqlCommand myCommandUserFilmssummfilmsUser1IdTanimoto = new MySqlCommand(queryStringUserFilmssummfilmsUser1IdTanimoto, myConnection);
                    myCommandUserFilmssummfilmsUser1IdTanimoto.CommandTimeout = 0;
                    myCommandUserFilmssummfilmsUser1IdTanimoto.ExecuteNonQuery();
                    string queryStringUserFilmssummfilmsUser1IdTanimoto1 = @"insert into userfilmssummfilms
                                                                        select a1.userId as user1Id, a2.userId as user2Id, (a1.Kolvo + a2.Kolvo) as p2
                                                                        from userfilmskolvoclaster1 as a1 join userfilmskolvoclaster1 as a2
                                                                        where a2.userId=" + user1IdTanimoto + @" and " + user1IdTanimoto + @">a1.userId";
                    MySqlCommand myCommandUserFilmssummfilmsUser1IdTanimoto1 = new MySqlCommand(queryStringUserFilmssummfilmsUser1IdTanimoto1, myConnection);
                    myCommandUserFilmssummfilmsUser1IdTanimoto1.CommandTimeout = 0;
                    myCommandUserFilmssummfilmsUser1IdTanimoto1.ExecuteNonQuery();
                
                    //userfilmsPeresech
                    //добавление записей с user1IdTanimoto
                    string queryStringUserFilmsPeresechUser1IdTanimoto = @"insert into userfilmsPeresech
                                                                        select t.User1Id, t.User2Id, count(*) as p1
                                                                        from (select a1.userId as User1Id, a2.userId as User2Id, a1.filmId as filmId
                                                                              from ratingsduplicate as a1 inner join ratingsduplicate as a2 on a1.filmId=a2.filmId 
                                                                              where a1.userId=" + user1IdTanimoto + @" and " + user1IdTanimoto + @"<a2.userId) as t
                                                                        group by t.User1Id, t.User2Id";
                    MySqlCommand myCommandUserFilmsPeresechUser1IdTanimoto = new MySqlCommand(queryStringUserFilmsPeresechUser1IdTanimoto, myConnection);
                    myCommandUserFilmsPeresechUser1IdTanimoto.CommandTimeout = 0;
                    myCommandUserFilmsPeresechUser1IdTanimoto.ExecuteNonQuery();
                    string queryStringUserFilmsPeresechUser1IdTanimoto1 = @"insert into userfilmsPeresech
                                                                        select t.User1Id, t.User2Id, count(*) as p1
                                                                        from (select a1.userId as User1Id, a2.userId as User2Id, a1.filmId as filmId
                                                                              from ratingsduplicate as a1 inner join ratingsduplicate as a2 on a1.filmId=a2.filmId 
                                                                              where a2.userId=" + user1IdTanimoto + @" and " + user1IdTanimoto + @">a1.userId) as t
                                                                        group by t.User1Id, t.User2Id";
                    MySqlCommand myCommandUserFilmsPeresechUser1IdTanimoto1 = new MySqlCommand(queryStringUserFilmsPeresechUser1IdTanimoto1, myConnection);
                    myCommandUserFilmsPeresechUser1IdTanimoto1.CommandTimeout = 0;
                    myCommandUserFilmsPeresechUser1IdTanimoto1.ExecuteNonQuery();

                

                    //userfilmsobiedinenie
                    //удаление
                    string queryStringDeleteUpominaniyaIsuserfilmsobiedinenie = @"delete from userfilmsobiedinenie 
                                                                                       where user1Id in (" + user1IdTanimoto + @", " + user2IdTanimoto + @")" +
                                                                                          @" or user2Id in (" + user1IdTanimoto + @", " + user2IdTanimoto + @")";
                    MySqlCommand myCommandDeleteUpominaniyaIsuserfilmsobiedinenie = new MySqlCommand(queryStringDeleteUpominaniyaIsuserfilmsobiedinenie, myConnection);
                    myCommandDeleteUpominaniyaIsuserfilmsobiedinenie.CommandTimeout = 0;
                    myCommandDeleteUpominaniyaIsuserfilmsobiedinenie.ExecuteNonQuery();

                    //добавление
                    string queryStringuserfilmsobiedineniehelp = @"insert into userfilmsobiedinenie select a1.user1Id, a1.user2Id, (a2.p2 - a1.p1) as p3
                                                                                                from userfilmsperesech as a1 inner join userfilmssummfilms as a2 
                                                                                                on a1.user1Id=" + user1IdTanimoto + @" and a2.user1Id=" + user1IdTanimoto + @" and a1.user2Id=a2.user2Id";
                    MySqlCommand myCommanduserfilmsobiedineniehelp = new MySqlCommand(queryStringuserfilmsobiedineniehelp, myConnection);
                    myCommanduserfilmsobiedineniehelp.CommandTimeout = 0;
                    myCommanduserfilmsobiedineniehelp.ExecuteNonQuery();
                    string queryStringuserfilmsobiedinenie2 = @"insert into userfilmsobiedinenie select a1.user1Id, a1.user2Id, (a2.p2 - a1.p1) as p3
                                                                                                from userfilmsperesech as a1 inner join userfilmssummfilms as a2 
                                                                                                on a1.user2Id=" + user1IdTanimoto + @" and a2.user2Id=" + user1IdTanimoto + @" and a1.user1Id=a2.user1Id";
                    MySqlCommand myCommanduserfilmsobiedinenie2 = new MySqlCommand(queryStringuserfilmsobiedinenie2, myConnection);
                    myCommanduserfilmsobiedinenie2.CommandTimeout = 0;
                    myCommanduserfilmsobiedinenie2.ExecuteNonQuery();
                    
                    //distances1
                    //удаление записей содержащие user1IdTanimoto или user2IdTanimoto
                    string queryStringDeleteUpominaniyaIsDistances1 = @"delete from distances1 
                                                                                       where user1Id in (" + user1IdTanimoto + @", " + user2IdTanimoto + @")" +
                                                                                          @" or user2Id in (" + user1IdTanimoto + @", " + user2IdTanimoto + @")";
                    MySqlCommand myCommandDeleteUpominaniyaIsDistances1 = new MySqlCommand(queryStringDeleteUpominaniyaIsDistances1, myConnection);
                    myCommandDeleteUpominaniyaIsDistances1.CommandTimeout = 0;
                    myCommandDeleteUpominaniyaIsDistances1.ExecuteNonQuery();
                    //добавление записей с user1IdTanimoto
                    string queryStringDistances1User1IdTanimoto = @"insert into distances1 
                                                                        select a1.user1Id, a1.user2Id, (a1.p1 / a2.p3) as distance1
                                                                        from userfilmsPeresech as a1 inner join userfilmsobiedinenie as a2 on 
                                                                        a1.user1Id=a2.user1Id and a1.user2Id=a2.user2Id
                                                                        where a1.user1Id=" + user1IdTanimoto + " or a1.user2Id=" + user1IdTanimoto;
                    MySqlCommand myCommandDistances1User1IdTanimoto = new MySqlCommand(queryStringDistances1User1IdTanimoto, myConnection);
                    myCommandDistances1User1IdTanimoto.CommandTimeout = 0;
                    myCommandDistances1User1IdTanimoto.ExecuteNonQuery();
                    
                    //удаляем записи пар пользователей, которые не будем объединять в один кластер  
                    string queryStringDeleteKTanimoto = @"delete from distances1 where distance1<0.2";
                    MySqlCommand myCommandDeleteKTanimoto = new MySqlCommand(queryStringDeleteKTanimoto, myConnection);
                    myCommandDeleteKTanimoto.CommandTimeout = 0;
                    myCommandDeleteKTanimoto.ExecuteNonQuery();

                    //очищаем таблицы
                    myCommandDeleteTableUserFilmsPeresech.ExecuteNonQuery();
                    myCommandDeleteTableUserFilmssummfilms.ExecuteNonQuery();

                }

                else 
                {
                    //удаляем коэффициент Танимото из таблицы distances1
                    string queryStringDeleteKTanimoto = @"delete from distances1 where user1Id=" + user1IdTanimoto + @" and user2id=" + user2IdTanimoto;
                    MySqlCommand myCommandDeleteKTanimoto = new MySqlCommand(queryStringDeleteKTanimoto, myConnection);
                    myCommandDeleteKTanimoto.CommandTimeout = 0;
                    myCommandDeleteKTanimoto.ExecuteNonQuery();
                    Console.Write("\n");
                    Console.Write("    ne obiedinim");
                
                }


                string queryStringcountStringIndistances1 = @"select count(*) from distances1";
                MySqlCommand myCommandcountStringIndistances1 = new MySqlCommand(queryStringcountStringIndistances1, myConnection);
                myCommandcountStringIndistances1.CommandTimeout = 0;
                string stringcountStringIndistances1 = myCommandcountStringIndistances1.ExecuteScalar().ToString();
                int countStringIndistances1 = Convert.ToInt32(stringcountStringIndistances1);

                if (countStringIndistances1 != 0)
                {
                    //определение максимального коэффициента  
                    MySqlDataReader MaxK1I = myCommandMaxKTanimoto.ExecuteReader();

                    MaxK1I.Read();
                    user1IdTanimoto = MaxK1I.GetString(0);
                    user2IdTanimoto = MaxK1I.GetString(1);
                    KTanimoto = MaxK1I.GetDouble(2);
                    MaxK1I.Close();
                }

                
                else
                {
                    KTanimoto = 0; 
                }

                Console.Write("\n");
                Console.Write("\n");
                k++;
                Console.WriteLine(k);
                DateTime time6 = DateTime.Now;
                Console.WriteLine(time6);
                Console.WriteLine(KTanimoto);
                
             }
            Console.WriteLine("Tanimoto end");
            Console.ReadLine();
            

            
            */
            







            //ВТОРАЯ КЛАСТЕРИЗАЦИЯ (EuclideanDistance)





            //определяем кол-во получившихся кластеров после кластеризации по коэффициенту Танимото
            string queryStringKolvoClasterov = @"select count(*)
                                                from (select *
		                                              from clasters1
		                                              group by claster1Id) as t";
            MySqlCommand myCommandKolvoClasterov = new MySqlCommand(queryStringKolvoClasterov, myConnection);
            myCommandKolvoClasterov.CommandTimeout = 0;
            string stringKolvoClasterov = myCommandKolvoClasterov.ExecuteScalar().ToString();
            int KolvoClasterov = Convert.ToInt32(stringKolvoClasterov);
            Console.Write(KolvoClasterov);

            //создаем массив с id кластеров
            string[] NomerClastera = new string[KolvoClasterov];
            string queryStringClasters1Id = @"select claster1Id
		                                     from clasters1
		                                     group by claster1Id";

            MySqlCommand myCommandClasters1Id = new MySqlCommand(queryStringClasters1Id, myConnection);
            myCommandClasters1Id.CommandTimeout = 0;
            MySqlDataReader Clasters1Id = myCommandClasters1Id.ExecuteReader();
            int i = 0;
            while (Clasters1Id.Read())
            {
                NomerClastera[i] = Clasters1Id.GetString(0);
                i++;
            }
            Clasters1Id.Close();

            for (i = 0; i <= KolvoClasterov - 1; i++)
            {
                //в таблицу ratingsduplicate2 добавляем rating пользователей из рассматриваемого кластера
                string queryStringRatingFilmovUserovClastera = @"insert into ratingsduplicate2 
                                                                            select * 
                                                                            from ratingsforclaster 
                                                                            where userid in (select userid from clasters1 where claster1id=" + NomerClastera[i] + @") or userid=" + NomerClastera[i];
                MySqlCommand myCommandRatingFilmovUserovClastera = new MySqlCommand(queryStringRatingFilmovUserovClastera, myConnection);
                myCommandRatingFilmovUserovClastera.CommandTimeout = 0;
                myCommandRatingFilmovUserovClastera.ExecuteNonQuery();

                //копируем таблицу ratingsduplicate2 (понадобится для определения средней оценки по фильму который смотрели несколько человек из кластера)
                string queryStringCopyRatingsduplicate = @"insert into ratingsduplicatesrednee select * from ratingsduplicate2";
                MySqlCommand myCommandCopyRatingsduplicate = new MySqlCommand(queryStringCopyRatingsduplicate, myConnection);
                myCommandCopyRatingsduplicate.CommandTimeout = 0;
                myCommandCopyRatingsduplicate.ExecuteNonQuery();
              

                //определяем кол-во фильмов в кластере
                string queryStringCountFilmsInClaster = @"select kolvo from userfilmskolvoclaster1 where userid=" + NomerClastera[i];
                MySqlCommand myCommandCountFilmsInClaster = new MySqlCommand(queryStringCountFilmsInClaster, myConnection);
                myCommandCountFilmsInClaster.CommandTimeout = 0;
                string CountFilmsInClasterString = myCommandCountFilmsInClaster.ExecuteScalar().ToString();
                int CountFilmsInClaster = Convert.ToInt32(CountFilmsInClasterString);
                //нижняя граница для коэффициента EuclideanDistance (необходима для кластеризации)
                double MinMaxEuclideanDistance = 1 / (1 + Math.Sqrt(6 * CountFilmsInClaster));
                Console.WriteLine("MinMaxEuclideanDistance");
                Console.WriteLine(MinMaxEuclideanDistance);


                //определяем расстояние между всевозможными парами пользователей (заносим информацию в таблицу distances2)
                string queryStringEuclideanDistance = @"insert into distances2 
                                                            select t.user1Id, t.user2Id, (1/(1+(sqrt(sum(t.raznost2))))) as distance2
                                                            from (select a1.userId as User1Id, a2.userId as User2Id, (a1.rating - a2.rating)*(a1.rating - a2.rating) as raznost2
                                                                  from ratingsduplicate2 as a1 inner join ratingsduplicate2 as a2 on a1.filmId=a2.filmId and a1.userId<a2.userId)as t
                                                            group by t.user1Id, t.user2Id";
                MySqlCommand myCommandEuclideanDistance = new MySqlCommand(queryStringEuclideanDistance, myConnection);
                myCommandEuclideanDistance.CommandTimeout = 0;
                myCommandEuclideanDistance.ExecuteNonQuery();

                

                //определение макс коэффициента
                string queryStringMaxEuclideanDistance = @"select * 
                                                               from distances2
                                                               where distance2=(select max(distance2) from distances2)";
                MySqlCommand myCommandMaxEuclideanDistance = new MySqlCommand(queryStringMaxEuclideanDistance, myConnection);
                myCommandMaxEuclideanDistance.CommandTimeout = 0;
                MySqlDataReader MaxEuclideanDistance = myCommandMaxEuclideanDistance.ExecuteReader();

                MaxEuclideanDistance.Read();
                string user1IdEuclidean = MaxEuclideanDistance.GetString(0);
                string user2IdEuclidean = MaxEuclideanDistance.GetString(1);
                double KEuclidean = MaxEuclideanDistance.GetDouble(2);
                MaxEuclideanDistance.Close();


                while (KEuclidean >= MinMaxEuclideanDistance)
                {
                    //всех пользователей кластера 2 (user2Id) определяем в 1 кластер (User1Id)
                    string queryStringUsersClaster2InClaster1Euclidean = @"update clasters2 set claster2Id=" + user1IdEuclidean + @" where claster2Id=" + user2IdEuclidean;
                    MySqlCommand myCommandUsersClaster2InClaster1Euclidean = new MySqlCommand(queryStringUsersClaster2InClaster1Euclidean, myConnection);
                    myCommandUsersClaster2InClaster1Euclidean.CommandTimeout = 0;
                    myCommandUsersClaster2InClaster1Euclidean.ExecuteNonQuery();

                    //объеденяем user1Id и user2Id в кластер       
                    string queryStringUser2IdInClaster1Euclidean = @"Insert into clasters2 values(" + user1IdEuclidean + @"," + user2IdEuclidean + @");";
                    MySqlCommand myCommandUser2IdInClaster1Euclidean = new MySqlCommand(queryStringUser2IdInClaster1Euclidean, myConnection);
                    myCommandUser2IdInClaster1Euclidean.CommandTimeout = 0;
                    myCommandUser2IdInClaster1Euclidean.ExecuteNonQuery();

                    //объединение фильмов 1-ого и 2-ого кластера.
                    string queryStringObedinenieOcenokClacterovEuclidean = @"INSERT INTO ratingsduplicate2 
                                                               (select " + user1IdEuclidean + @", filmId, rating from ratingsduplicate2 where userId=" + user2IdEuclidean + @")
                                                               ON DUPLICATE key update userId=" + user1IdEuclidean;
                    MySqlCommand myCommandObedinenieOcenokClacterovEuclidean = new MySqlCommand(queryStringObedinenieOcenokClacterovEuclidean, myConnection);
                    myCommandObedinenieOcenokClacterovEuclidean.CommandTimeout = 0;
                    myCommandObedinenieOcenokClacterovEuclidean.ExecuteNonQuery();
                    string queryStringUdalenieOcenok2ClasteraEuclidean = @"delete from ratingsduplicate2 where userId=" + user2IdEuclidean;
                    MySqlCommand myCommandUdalenieOcenok2ClasteraEuclidean = new MySqlCommand(queryStringUdalenieOcenok2ClasteraEuclidean, myConnection);
                    myCommandUdalenieOcenok2ClasteraEuclidean.CommandTimeout = 0;
                    myCommandUdalenieOcenok2ClasteraEuclidean.ExecuteNonQuery();


                    //добавляем записи-усреднение оценок по фильмам которые смотрели несколько пользователей.
                    //удаляем оценки фильмов, которые посмотрели в кластере больше 1 человека
                    string queryStringDeleteDoubleFromRatingsduplicate = @"delete from ratingsduplicate2 
                                                                           where filmId in (select filmId
                                                                                            from (select *, count(filmId) as count
                                                                                                  from ratingsduplicatesrednee
                                                                                                  where userId in(select userId
					                                                                                              from clasters2
					                                                                                              where claster2Id=" + user1IdEuclidean + @") or userId=" + user1IdEuclidean + @"
                                                                                                  group by filmid) as a
                                                                                            where a.count>1)";
                    MySqlCommand myCommandDeleteDoubleFromRatingsduplicate = new MySqlCommand(queryStringDeleteDoubleFromRatingsduplicate, myConnection);
                    myCommandDeleteDoubleFromRatingsduplicate.CommandTimeout = 0;
                    myCommandDeleteDoubleFromRatingsduplicate.ExecuteNonQuery();

                    //записываем оценки фильмов, которые посмотрели в кластере больше 1 человека(средняя оценка всех пользователей, которые смотрели фильм)
                    string queryStringInsertDoubleFromRatingsduplicate = @"insert into ratingsduplicate2 select " + user1IdEuclidean + @" as userId, a.filmId, a.sum/a.count  as rating
                                                                                                        from (select *, count(filmId) as count, sum(rating) as sum
                                                                                                              from ratingsduplicatesrednee
                                                                                                              where userId in(select userId
					                                                                                                          from clasters2
					                                                                                                          where claster2Id=" + user1IdEuclidean + @") or userId=" + user1IdEuclidean + @"
                                                                                                              group by filmid) as a
                                                                                                        where a.count>1";
                    MySqlCommand myCommandInsertDoubleFromRatingsduplicate = new MySqlCommand(queryStringInsertDoubleFromRatingsduplicate, myConnection);
                    myCommandInsertDoubleFromRatingsduplicate.CommandTimeout = 0;
                    myCommandInsertDoubleFromRatingsduplicate.ExecuteNonQuery();


                    //удаляем все упоминания о user1IdEuclidean и user2IdEuclidean из таблицы distances2
                    //и добавляем записи кластера user1IdEuclidean, для определения новых коэффициентов EuclideanDistance                    
                    //удаление записей содержащие user1IdEuclidean или user2IdEuclidean
                    string queryStringDeleteUpominaniyaIsDistances2 = @"delete from distances2 
                                                                                   where user1Id in (" + user1IdEuclidean + @", " + user2IdEuclidean + @")" +
                                                                                          @" or user2Id in (" + user1IdEuclidean + @", " + user2IdEuclidean + @")";
                    MySqlCommand myCommandDeleteUpominaniyaIsDistances2 = new MySqlCommand(queryStringDeleteUpominaniyaIsDistances2, myConnection);
                    myCommandDeleteUpominaniyaIsDistances2.CommandTimeout = 0;
                    myCommandDeleteUpominaniyaIsDistances2.ExecuteNonQuery();
                    //добавление записей с user1IdEuclidean
                    string queryStringDistances2user1IdEuclidean = @"insert into distances2 
                                                                        select t.user1Id, t.user2Id, (1/(1+(sqrt(sum(t.raznost2))))) as distance2
                                                                        from (select a1.userId as User1Id, a2.userId as User2Id, (a1.rating - a2.rating)*(a1.rating - a2.rating) as raznost2
                                                                              from ratingsduplicate2 as a1 inner join ratingsduplicate2 as a2 on a1.filmId=a2.filmId 
		                                                                      where (a1.userId=" + user1IdEuclidean + @" and a1.userId<a2.userId) or (a2.userId=" + user1IdEuclidean + @" and a1.userId<a2.userId))as t
                                                                              group by t.user1Id, t.user2Id";
                    MySqlCommand myCommandDistances2user1IdEuclidean = new MySqlCommand(queryStringDistances2user1IdEuclidean, myConnection);
                    myCommandDistances2user1IdEuclidean.CommandTimeout = 0;
                    myCommandDistances2user1IdEuclidean.ExecuteNonQuery();      






                    //проверка:остались ли записи в таблице 
                    string queryStringCountStringInDistances2 = @"select count(*) from distances2";
                    MySqlCommand myCommandCountStringInDistances2 = new MySqlCommand(queryStringCountStringInDistances2, myConnection);
                    myCommandCountStringInDistances2.CommandTimeout = 0;
                    string CountStringInDistances2String = myCommandCountStringInDistances2.ExecuteScalar().ToString();
                    int CountStringInDistances2 = Convert.ToInt32(CountStringInDistances2String);

                    if (CountStringInDistances2 > 0)
                    {
                        //определение макс коэффициента EuclideanDistance
                        MaxEuclideanDistance = myCommandMaxEuclideanDistance.ExecuteReader();

                        MaxEuclideanDistance.Read();
                        user1IdEuclidean = MaxEuclideanDistance.GetString(0);
                        user2IdEuclidean = MaxEuclideanDistance.GetString(1);
                        KEuclidean = MaxEuclideanDistance.GetDouble(2);
                        MaxEuclideanDistance.Close();

                        Console.WriteLine(KEuclidean);
                        Console.WriteLine("1");

                    }
                    else
                    {
                        MinMaxEuclideanDistance = 2;
                        Console.WriteLine("2");                    
                    }
                }
                Console.WriteLine("razbit na podclasters");

                //очищаем таблицу ratingsduplicate, ratingsduplicatesrednee, distances2 для разбиения следующего кластера
                myCommandDeleteTableeRatingsduplicate2.ExecuteNonQuery();
                myCommandDeleteTableDistances2.ExecuteNonQuery();
                myCommandDeleteTableRatingsduplicateSrednee.ExecuteNonQuery();
            }

            myConnection.Close();

            
        }
    }
}
